package anthropic

import (
	"fmt"
	"strings"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
	"github.com/rohanreddy79/llm-gateway/internal/provider"
)

// toMessagesRequest translates an OpenAI chat request into a Messages API
// request. System/developer turns become the top-level `system` field,
// consecutive same-role turns are merged (the Messages API requires strict
// alternation) and max_tokens is defaulted because Anthropic requires it.
func toMessagesRequest(name string, req *chat.Request, defaultMaxTokens int) (*messagesRequest, error) {
	var system []string
	var msgs []message
	for i, m := range req.Messages {
		if len(m.ToolCalls) > 0 || m.ToolCallID != "" {
			return nil, provider.BadRequest(name, fmt.Sprintf("messages[%d]: tool calls are not supported by the anthropic translation", i))
		}
		text, ok := m.Content.PlainText()
		if !ok {
			return nil, provider.BadRequest(name, fmt.Sprintf("messages[%d]: only text content parts are supported by the anthropic translation", i))
		}
		switch m.Role {
		case "system", "developer":
			system = append(system, text)
		case "user", "assistant":
			if n := len(msgs); n > 0 && msgs[n-1].Role == m.Role {
				msgs[n-1].Content += "\n\n" + text
				continue
			}
			msgs = append(msgs, message{Role: m.Role, Content: text})
		default:
			return nil, provider.BadRequest(name, fmt.Sprintf("messages[%d]: role %q is not supported by the anthropic translation", i, m.Role))
		}
	}
	if len(msgs) == 0 {
		return nil, provider.BadRequest(name, "messages must contain at least one user or assistant message")
	}
	out := &messagesRequest{
		Model:         req.Model,
		MaxTokens:     req.MaxOutputTokens(),
		System:        strings.Join(system, "\n\n"),
		Messages:      msgs,
		TopP:          req.TopP,
		StopSequences: []string(req.Stop),
		Stream:        req.Stream,
	}
	if out.MaxTokens <= 0 {
		out.MaxTokens = defaultMaxTokens
	}
	if req.Temperature != nil {
		// OpenAI's range is [0,2], Anthropic's is [0,1]; clamp rather than reject.
		t := *req.Temperature
		if t > 1 {
			t = 1
		}
		if t < 0 {
			t = 0
		}
		out.Temperature = &t
	}
	if req.User != "" {
		out.Metadata = &metadata{UserID: req.User}
	}
	return out, nil
}

// finishReason maps Anthropic stop reasons onto OpenAI finish reasons.
func finishReason(stop string) string {
	switch stop {
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	case "refusal":
		return "content_filter"
	default: // end_turn, stop_sequence, pause_turn, ""
		return "stop"
	}
}

// toUsage converts token accounting; cached reads are surfaced the OpenAI way.
func toUsage(u usage) *chat.Usage {
	out := &chat.Usage{
		PromptTokens:     u.promptTokens(),
		CompletionTokens: u.OutputTokens,
	}
	out.TotalTokens = out.PromptTokens + out.CompletionTokens
	if u.CacheReadInputTokens > 0 {
		out.PromptTokensDetails = &chat.PromptTokensDetails{CachedTokens: u.CacheReadInputTokens}
	}
	return out
}

// fromMessagesResponse translates a non-streaming Messages response. Only
// text blocks are rendered; thinking and tool-use blocks are dropped.
func fromMessagesResponse(resp *messagesResponse, created time.Time) *chat.Response {
	var sb strings.Builder
	for _, b := range resp.Content {
		if b.Type == "text" {
			sb.WriteString(b.Text)
		}
	}
	return &chat.Response{
		ID:      resp.ID,
		Object:  "chat.completion",
		Created: created.Unix(),
		Model:   resp.Model,
		Choices: []chat.Choice{{
			Index:        0,
			Message:      chat.Message{Role: "assistant", Content: chat.Content{Text: sb.String()}},
			FinishReason: chat.StrPtr(finishReason(resp.StopReason)),
		}},
		Usage: toUsage(resp.Usage),
	}
}
