// Package usage keeps per-virtual-key accounting in memory: request counts,
// tokens and estimated cost, with a per-upstream-model breakdown.
package usage

import (
	"math"
	"sort"
	"sync"
)

// Record is one accounting bucket.
type Record struct {
	Requests     int64   `json:"requests"`
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	CostUSD      float64 `json:"cost_usd"`
	// UnpricedRequests counts requests whose model has no price entry; their
	// cost is not included in CostUSD.
	UnpricedRequests int64 `json:"unpriced_requests,omitempty"`
}

func (r *Record) add(in, out int, usd float64, priced bool) {
	r.Requests++
	r.InputTokens += int64(in)
	r.OutputTokens += int64(out)
	r.CostUSD += usd
	if !priced {
		r.UnpricedRequests++
	}
}

// KeyUsage is the report for one virtual key.
type KeyUsage struct {
	Key string `json:"key"`
	Record
	// Models is keyed by "provider:model".
	Models map[string]*Record `json:"models"`
}

// Report is the JSON body of GET /admin/usage.
type Report struct {
	Keys []KeyUsage `json:"keys"`
}

// Pricer returns the cost of a completion and whether the model is priced.
type Pricer func(model string, inputTokens, outputTokens int) (usd float64, priced bool)

// Tracker is safe for concurrent use.
type Tracker struct {
	mu    sync.Mutex
	price Pricer
	keys  map[string]*KeyUsage
}

// New returns an empty tracker.
func New(price Pricer) *Tracker {
	return &Tracker{price: price, keys: make(map[string]*KeyUsage)}
}

// Record adds one completion and returns its estimated cost.
func (t *Tracker) Record(keyName, provider, model string, inputTokens, outputTokens int) (usd float64, priced bool) {
	usd, priced = t.price(model, inputTokens, outputTokens)
	t.mu.Lock()
	defer t.mu.Unlock()
	ku, ok := t.keys[keyName]
	if !ok {
		ku = &KeyUsage{Key: keyName, Models: make(map[string]*Record)}
		t.keys[keyName] = ku
	}
	ku.Record.add(inputTokens, outputTokens, usd, priced)
	target := provider + ":" + model
	mr, ok := ku.Models[target]
	if !ok {
		mr = &Record{}
		ku.Models[target] = mr
	}
	mr.add(inputTokens, outputTokens, usd, priced)
	return usd, priced
}

// Snapshot returns a deep copy sorted by key name.
func (t *Tracker) Snapshot() Report {
	t.mu.Lock()
	defer t.mu.Unlock()
	rep := Report{Keys: make([]KeyUsage, 0, len(t.keys))}
	for _, ku := range t.keys {
		cp := KeyUsage{Key: ku.Key, Record: ku.Record, Models: make(map[string]*Record, len(ku.Models))}
		cp.CostUSD = roundUSD(cp.CostUSD)
		for k, v := range ku.Models {
			r := *v
			r.CostUSD = roundUSD(r.CostUSD)
			cp.Models[k] = &r
		}
		rep.Keys = append(rep.Keys, cp)
	}
	sort.Slice(rep.Keys, func(i, j int) bool { return rep.Keys[i].Key < rep.Keys[j].Key })
	return rep
}

// roundUSD trims float noise to nanodollars so the JSON reads as a plain
// decimal instead of 8.160000000000002e-05.
func roundUSD(v float64) float64 { return math.Round(v*1e9) / 1e9 }
