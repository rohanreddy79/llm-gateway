package usage

import (
	"sync"
	"testing"
)

func pricer(model string, in, out int) (float64, bool) {
	if model != "priced" {
		return 0, false
	}
	return float64(in)*1.0/1e6 + float64(out)*2.0/1e6, true
}

func TestRecordAndSnapshot(t *testing.T) {
	tr := New(pricer)
	if usd, priced := tr.Record("beta", "p", "priced", 1_000_000, 500_000); !priced || usd != 2.0 {
		t.Fatalf("Record = %v, %v", usd, priced)
	}
	tr.Record("beta", "p", "priced", 10, 10)
	tr.Record("beta", "q", "mystery", 5, 5)
	tr.Record("alpha", "p", "priced", 1, 1)
	rep := tr.Snapshot()
	if len(rep.Keys) != 2 || rep.Keys[0].Key != "alpha" || rep.Keys[1].Key != "beta" {
		t.Fatalf("keys = %+v", rep.Keys)
	}
	b := rep.Keys[1]
	if b.Requests != 3 || b.InputTokens != 1_000_015 || b.OutputTokens != 500_015 || b.UnpricedRequests != 1 {
		t.Errorf("beta totals = %+v", b.Record)
	}
	if b.CostUSD < 2.00003-1e-9 || b.CostUSD > 2.00003+1e-9 {
		t.Errorf("beta cost = %v", b.CostUSD)
	}
	if b.Models["q:mystery"].Requests != 1 || b.Models["p:priced"].Requests != 2 {
		t.Errorf("models = %+v", b.Models)
	}
	// The snapshot is a copy.
	b.Models["p:priced"].Requests = 99
	if tr.Snapshot().Keys[1].Models["p:priced"].Requests != 2 {
		t.Error("Snapshot must not alias internal state")
	}
}

func TestConcurrent(t *testing.T) {
	tr := New(pricer)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr.Record("k", "p", "priced", 1, 1)
		}()
	}
	wg.Wait()
	if got := tr.Snapshot().Keys[0].Requests; got != 100 {
		t.Fatalf("requests = %d", got)
	}
}
