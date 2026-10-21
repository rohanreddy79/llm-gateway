package config

// DefaultPrices is the built-in price table, USD per million tokens.
//
// Claude prices are Anthropic first-party API rates as listed by the claude-api
// skill reference on 2026-10-06. Cache-read and cache-write multipliers are
// not modelled: cached prompt tokens are billed here at the full input rate,
// so estimates are an upper bound. Prices for other vendors are deliberately
// absent; add them under `prices:` in gateway.yaml.
func DefaultPrices() map[string]Price {
	return map[string]Price{
		"claude-fable-5-1":  {Input: 10.00, Output: 50.00},
		"claude-fable-5":    {Input: 10.00, Output: 50.00},
		"claude-opus-5-5":   {Input: 4.00, Output: 20.00},
		"claude-opus-5":     {Input: 5.00, Output: 25.00},
		"claude-opus-4-8":   {Input: 5.00, Output: 25.00},
		"claude-opus-4-7":   {Input: 5.00, Output: 25.00},
		"claude-opus-4-6":   {Input: 5.00, Output: 25.00},
		"claude-sonnet-5-5": {Input: 2.00, Output: 10.00},
		"claude-sonnet-5":   {Input: 2.00, Output: 10.00},
		"claude-sonnet-4-6": {Input: 3.00, Output: 15.00},
		"claude-haiku-5-5":  {Input: 0.10, Output: 0.50},
		"claude-haiku-4-5":  {Input: 1.00, Output: 5.00},
	}
}

// Cost returns the estimated USD cost of a completion, and whether the model
// has a known price (a zero cost for an unpriced model is not "free").
func (c *Config) Cost(model string, inputTokens, outputTokens int) (usd float64, priced bool) {
	p, ok := c.Prices[model]
	if !ok {
		return 0, false
	}
	return float64(inputTokens)*p.Input/1e6 + float64(outputTokens)*p.Output/1e6, true
}
