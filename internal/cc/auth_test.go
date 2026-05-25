package cc

import (
	"testing"
	"time"
)

func TestStateStoreGenerateConsume(t *testing.T) {
	s := NewStateStore(time.Minute)
	tok, err := s.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("empty token")
	}
	if s.Len() != 1 {
		t.Fatalf("Len=%d, want 1", s.Len())
	}
	if !s.Consume(tok) {
		t.Fatal("first Consume should succeed")
	}
	if s.Consume(tok) {
		t.Fatal("second Consume should fail (single-shot)")
	}
	if s.Len() != 0 {
		t.Fatalf("Len=%d after consume, want 0", s.Len())
	}
}

func TestStateStoreRejectsUnknown(t *testing.T) {
	s := NewStateStore(time.Minute)
	if s.Consume("not-a-real-token") {
		t.Fatal("Consume of unknown token should fail")
	}
	if s.Consume("") {
		t.Fatal("Consume of empty string should fail")
	}
}

func TestStateStoreExpires(t *testing.T) {
	base := time.Now()
	s := NewStateStore(time.Minute)
	s.now = func() time.Time { return base }

	tok, err := s.Generate()
	if err != nil {
		t.Fatal(err)
	}

	// Just before TTL — still valid.
	s.now = func() time.Time { return base.Add(59 * time.Second) }
	if !s.Consume(tok) {
		t.Fatal("token within TTL should consume")
	}

	// Generate another and let it expire.
	tok2, _ := s.Generate()
	s.now = func() time.Time { return base.Add(2 * time.Minute) }
	if s.Consume(tok2) {
		t.Fatal("expired token should not consume")
	}
}
