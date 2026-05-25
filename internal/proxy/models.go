package proxy

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/cc"
)

// ModelsHandler serves GET /v1/models in the OpenAI list shape. Public
// — no bearer required, mainly so SDKs can introspect available
// models before authenticating.
type ModelsHandler struct{}

type modelListResponse struct {
	Object string            `json:"object"`
	Data   []modelDescriptor `json:"data"`
}

type modelDescriptor struct {
	ID            string `json:"id"`
	Object        string `json:"object"`
	Created       int64  `json:"created"`
	OwnedBy       string `json:"owned_by"`
	ContextWindow int    `json:"context_window,omitempty"`
	MaxTokens     int    `json:"max_completion_tokens,omitempty"`
}

func (h *ModelsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	created := time.Now().Unix()
	data := make([]modelDescriptor, 0, len(cc.GoModels))
	for _, m := range cc.GoModels {
		data = append(data, modelDescriptor{
			ID:            m.ID,
			Object:        "model",
			Created:       created,
			OwnedBy:       "commandcode-go",
			ContextWindow: m.ContextWindow,
			MaxTokens:     m.MaxTokens,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(modelListResponse{Object: "list", Data: data})
}
