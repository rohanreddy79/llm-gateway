package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/rohanreddy79/llm-gateway/internal/cache"
	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/ratelimit"
	"github.com/rohanreddy79/llm-gateway/internal/router"
	"github.com/rohanreddy79/llm-gateway/internal/sse"
)

// maxBodyBytes bounds a chat request; long conversations fit comfortably.
const maxBodyBytes = 10 << 20

func (g *Gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	breakers := make(map[string]string)
	for _, name := range g.router.Providers() {
		breakers[name] = g.router.BreakerState(name).String()
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "breakers": breakers})
}

func (g *Gateway) handleModels(w http.ResponseWriter, r *http.Request) {
	info := infoFrom(r.Context())
	list := chat.ModelList{Object: "list", Data: []chat.Model{}}
	for _, alias := range g.cfg.Aliases() {
		if !info.Key.AllowsModel(alias) {
			continue
		}
		list.Data = append(list.Data, chat.Model{ID: alias, Object: "model", Created: g.started.Unix(), OwnedBy: g.cfg.Routes[alias][0].Provider})
	}
	writeJSON(w, http.StatusOK, list)
}

func (g *Gateway) handleUsage(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, g.usage.Snapshot())
}

func (g *Gateway) handleChat(w http.ResponseWriter, r *http.Request) {
	info := infoFrom(r.Context())
	var req chat.Request
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_json", "Could not parse the JSON body: "+err.Error())
		return
	}
	if msg := validate(&req); msg != "" {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_request", msg)
		return
	}
	info.Model = req.Model
	if _, ok := g.cfg.Routes[req.Model]; !ok {
		writeError(w, http.StatusNotFound, "invalid_request_error", "model_not_found", fmt.Sprintf("The model `%s` does not exist.", req.Model))
		return
	}
	if !info.Key.AllowsModel(req.Model) {
		writeError(w, http.StatusForbidden, "permission_error", "model_not_allowed", fmt.Sprintf("Key %q is not allowed to use model `%s`.", info.KeyName, req.Model))
		return
	}

	cacheable := g.cache.Enabled() && cache.Cacheable(&req)
	var cacheKey string
	if cacheable {
		cacheKey = cache.Key(&req)
		if body, ok := g.cache.Get(cacheKey); ok {
			g.metrics.CacheEvents.WithLabelValues("hit").Inc()
			info.Cache, info.Provider = "hit", "cache"
			w.Header().Set("X-Cache", "HIT")
			w.Header().Set("Content-Type", "application/json")
			w.Write(body)
			return
		}
		g.metrics.CacheEvents.WithLabelValues("miss").Inc()
		info.Cache = "miss"
	} else {
		g.metrics.CacheEvents.WithLabelValues("bypass").Inc()
	}

	if req.Stream {
		g.streamChat(w, r, info, &req)
		return
	}
	resp, res, err := g.router.Complete(r.Context(), req.Model, &req)
	info.Provider, info.Attempts, info.Fallbacks = res.Provider, res.Attempts, res.Fallbacks
	if err != nil {
		g.writeUpstreamError(w, info, err)
		return
	}
	body := resp.Raw
	if body == nil {
		body, err = json.Marshal(resp)
		if err != nil {
			g.writeUpstreamError(w, info, err)
			return
		}
	}
	if cacheable {
		g.cache.Set(cacheKey, body)
		w.Header().Set("X-Cache", "MISS")
	}
	g.account(info, res, resp.Usage)
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

// validate returns a message for requests the gateway cannot serve.
func validate(req *chat.Request) string {
	switch {
	case req.Model == "":
		return "model is required."
	case len(req.Messages) == 0:
		return "messages must contain at least one message."
	case req.N != nil && *req.N != 1:
		return "n must be 1; the gateway does not fan out multiple candidates."
	}
	for i, m := range req.Messages {
		if m.Role == "" {
			return fmt.Sprintf("messages[%d]: role is required.", i)
		}
	}
	return ""
}

// streamChat relays chunks as `data: {json}` events and ends with [DONE].
// Errors before the first byte are ordinary JSON errors; errors mid-stream
// are sent as a final data event with an error object, which is what the
// OpenAI SDKs expect.
func (g *Gateway) streamChat(w http.ResponseWriter, r *http.Request, info *reqInfo, req *chat.Request) {
	ch, res, err := g.router.Stream(r.Context(), req.Model, req)
	info.Provider, info.Attempts, info.Fallbacks = res.Provider, res.Attempts, res.Fallbacks
	if err != nil {
		g.writeUpstreamError(w, info, err)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	rc.Flush()

	var lastUsage *chat.Usage
	for c := range ch {
		if c.Err != nil {
			_, typ, code, msg := upstreamError(c.Err)
			info.Err = c.Err.Error()
			body, _ := json.Marshal(chat.ErrorResponse{Error: chat.ErrorBody{Message: msg, Type: typ, Code: &code}})
			w.Write(sse.Format("", string(body)))
			break
		}
		body := c.Data.Raw
		if body == nil {
			body, _ = json.Marshal(c.Data)
		}
		if _, werr := w.Write(sse.Format("", string(body))); werr != nil {
			info.Err = "client disconnected"
			return
		}
		rc.Flush()
		if c.Data.Usage != nil {
			lastUsage = c.Data.Usage
		}
	}
	w.Write(sse.Format("", "[DONE]"))
	rc.Flush()
	g.account(info, res, lastUsage)
}

// account records tokens and cost against the key and debits its TPM bucket.
func (g *Gateway) account(info *reqInfo, res router.Result, u *chat.Usage) {
	if u == nil {
		return
	}
	info.Usage = u
	usd, _ := g.usage.Record(info.KeyName, res.Provider, res.Model, u.PromptTokens, u.CompletionTokens)
	g.metrics.Tokens.WithLabelValues(info.KeyName, info.Model, "input").Add(float64(u.PromptTokens))
	g.metrics.Tokens.WithLabelValues(info.KeyName, info.Model, "output").Add(float64(u.CompletionTokens))
	g.metrics.Cost.WithLabelValues(info.KeyName, info.Model).Add(usd)
	total := u.TotalTokens
	if total == 0 {
		total = u.PromptTokens + u.CompletionTokens
	}
	g.limiter.RecordTokens(info.keySecret, ratelimit.Limit{RPM: info.Key.RPM, TPM: info.Key.TPM}, total)
}

func (g *Gateway) writeUpstreamError(w http.ResponseWriter, info *reqInfo, err error) {
	status, typ, code, msg := upstreamError(err)
	info.Err = err.Error()
	writeError(w, status, typ, code, msg)
}
