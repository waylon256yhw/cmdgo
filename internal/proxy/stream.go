package proxy

import "github.com/waylon256yhw/cmdgo/internal/cc"

// eventStream is the minimal surface the OpenAI / Anthropic stream
// translators need. cc.Scanner satisfies it directly; prefixedStream
// wraps a pre-consumed first event ahead of a real scanner so the
// retry loop can peek the first frame for retry decisions without
// the adapter ever knowing.
type eventStream interface {
	Next() (*cc.StreamEvent, error)
}

// prefixedStream yields `first` once (if non-nil) and then delegates
// to `inner`.
type prefixedStream struct {
	first     *cc.StreamEvent
	delivered bool
	inner     *cc.Scanner
}

func newPrefixedStream(first *cc.StreamEvent, inner *cc.Scanner) *prefixedStream {
	return &prefixedStream{first: first, inner: inner}
}

func (p *prefixedStream) Next() (*cc.StreamEvent, error) {
	if !p.delivered {
		p.delivered = true
		if p.first != nil {
			return p.first, nil
		}
	}
	return p.inner.Next()
}
