package anthropic

// Wire types for the Anthropic Messages API (anthropic-version 2023-06-01),
// limited to what the gateway translates.

type messagesRequest struct {
	Model         string    `json:"model"`
	MaxTokens     int       `json:"max_tokens"`
	System        string    `json:"system,omitempty"`
	Messages      []message `json:"messages"`
	Temperature   *float64  `json:"temperature,omitempty"`
	TopP          *float64  `json:"top_p,omitempty"`
	StopSequences []string  `json:"stop_sequences,omitempty"`
	Stream        bool      `json:"stream,omitempty"`
	Metadata      *metadata `json:"metadata,omitempty"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type metadata struct {
	UserID string `json:"user_id,omitempty"`
}

type messagesResponse struct {
	ID           string         `json:"id"`
	Type         string         `json:"type"`
	Role         string         `json:"role"`
	Model        string         `json:"model"`
	Content      []contentBlock `json:"content"`
	StopReason   string         `json:"stop_reason"`
	StopSequence *string        `json:"stop_sequence"`
	Usage        usage          `json:"usage"`
}

type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type usage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens,omitempty"`
}

// promptTokens is OpenAI's prompt_tokens: Anthropic's input_tokens excludes
// cache reads and writes, OpenAI's includes them.
func (u usage) promptTokens() int {
	return u.InputTokens + u.CacheCreationInputTokens + u.CacheReadInputTokens
}

type apiError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type errorResponse struct {
	Type  string   `json:"type"`
	Error apiError `json:"error"`
}

// streamEvent is the union of every SSE payload shape; the Type field says
// which fields are populated.
type streamEvent struct {
	Type         string            `json:"type"`
	Message      *messagesResponse `json:"message,omitempty"`
	Index        int               `json:"index"`
	ContentBlock *contentBlock     `json:"content_block,omitempty"`
	Delta        *streamDelta      `json:"delta,omitempty"`
	Usage        *usage            `json:"usage,omitempty"`
	Error        *apiError         `json:"error,omitempty"`
}

type streamDelta struct {
	Type         string  `json:"type"`
	Text         string  `json:"text,omitempty"`
	StopReason   string  `json:"stop_reason,omitempty"`
	StopSequence *string `json:"stop_sequence,omitempty"`
}
