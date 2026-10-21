// Package cache is a bounded LRU response cache with a TTL. Only requests
// whose output is deterministic are cached; see Cacheable and Key.
package cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/rohanreddy79/llm-gateway/internal/chat"
)

// Cache stores response bodies keyed by Key. The zero MaxEntries disables it.
type Cache struct {
	mu    sync.Mutex
	ttl   time.Duration
	max   int
	ll    *list.List
	items map[string]*list.Element
	now   func() time.Time
}

type entry struct {
	key     string
	value   []byte
	expires time.Time
}

// New creates a cache; now may be nil.
func New(ttl time.Duration, maxEntries int, now func() time.Time) *Cache {
	if now == nil {
		now = time.Now
	}
	return &Cache{ttl: ttl, max: maxEntries, ll: list.New(), items: make(map[string]*list.Element), now: now}
}

// Enabled reports whether Set stores anything.
func (c *Cache) Enabled() bool { return c != nil && c.max > 0 }

// Get returns a copy-free view of the cached body. Callers must not mutate it.
func (c *Cache) Get(key string) ([]byte, bool) {
	if !c.Enabled() {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	e := el.Value.(*entry)
	if !c.now().Before(e.expires) {
		c.ll.Remove(el)
		delete(c.items, key)
		return nil, false
	}
	c.ll.MoveToFront(el)
	return e.value, true
}

// Set stores value, evicting the least recently used entry when full.
func (c *Cache) Set(key string, value []byte) {
	if !c.Enabled() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	exp := c.now().Add(c.ttl)
	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry)
		e.value, e.expires = value, exp
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&entry{key: key, value: value, expires: exp})
	c.items[key] = el
	for c.ll.Len() > c.max {
		oldest := c.ll.Back()
		c.ll.Remove(oldest)
		delete(c.items, oldest.Value.(*entry).key)
	}
}

// Len is the number of stored entries, including not-yet-expired ones.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Cacheable reports whether a request's output is deterministic enough to
// cache: non-streaming, temperature zero or unset (OpenAI's default is 1, but
// most clients that want reproducible output pass 0 explicitly; treating
// "unset" as cacheable is a deliberate gateway policy, documented in README)
// and a single candidate.
func Cacheable(req *chat.Request) bool {
	if req.Stream {
		return false
	}
	if req.Temperature != nil && *req.Temperature != 0 {
		return false
	}
	if req.N != nil && *req.N != 1 {
		return false
	}
	return true
}

// keyFields is the normalised subset of a request that determines its output.
type keyFields struct {
	Model       string                     `json:"model"`
	Messages    []chat.Message             `json:"messages"`
	MaxTokens   int                        `json:"max_tokens"`
	Temperature float64                    `json:"temperature"`
	TopP        *float64                   `json:"top_p"`
	Stop        chat.Stop                  `json:"stop"`
	Extra       map[string]json.RawMessage `json:"extra"`
}

// Key hashes the fields that influence the completion. The caller's `user`
// and streaming flags are excluded; unknown passthrough fields are included
// because an upstream may honour them (e.g. response_format).
func Key(req *chat.Request) string {
	kf := keyFields{
		Model:     req.Model,
		Messages:  req.Messages,
		MaxTokens: req.MaxOutputTokens(),
		TopP:      req.TopP,
		Stop:      req.Stop,
		Extra:     req.Extra,
	}
	if req.Temperature != nil {
		kf.Temperature = *req.Temperature
	}
	b, err := json.Marshal(kf)
	if err != nil {
		// Only unmarshalable types could fail here, and keyFields has none.
		return ""
	}
	sum := sha256.Sum256(b)
	return "v1:" + hex.EncodeToString(sum[:])
}
