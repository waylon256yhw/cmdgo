package pool

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/waylon256yhw/cmdgo/internal/store"
)

func newStoreWithAccounts(t *testing.T, accs ...store.Account) *store.Store {
	t.Helper()
	st, err := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Update(func(s *store.State) error {
		s.Accounts = append(s.Accounts, accs...)
		return nil
	})
	return st
}

func TestPickReturnsAccount(t *testing.T) {
	st := newStoreWithAccounts(t, store.Account{
		ID: "a1", APIKey: "user_aaa11111", LastKnownCredits: 9.0,
	})
	p := New(st)
	got, err := p.Pick(PickOptions{ClientToken: "t", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "a1" {
		t.Errorf("got %q, want a1", got.ID)
	}
}

func TestPickAffinityStableAcrossCalls(t *testing.T) {
	st := newStoreWithAccounts(t,
		store.Account{ID: "a1", APIKey: "user_aaa11111", LastKnownCredits: 9},
		store.Account{ID: "a2", APIKey: "user_bbb22222", LastKnownCredits: 9},
		store.Account{ID: "a3", APIKey: "user_ccc33333", LastKnownCredits: 9},
	)
	p := New(st)
	first, err := p.Pick(PickOptions{ClientToken: "tok", Model: "deepseek/deepseek-v4-pro"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		got, err := p.Pick(PickOptions{ClientToken: "tok", Model: "deepseek/deepseek-v4-pro"})
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != first.ID {
			t.Fatalf("Pick %d returned %q, want %q (affinity broken)", i, got.ID, first.ID)
		}
	}
}

func TestPickDifferentInputsHitDifferentShards(t *testing.T) {
	st := newStoreWithAccounts(t,
		store.Account{ID: "a1", APIKey: "user_aaa11111", LastKnownCredits: 9},
		store.Account{ID: "a2", APIKey: "user_bbb22222", LastKnownCredits: 9},
		store.Account{ID: "a3", APIKey: "user_ccc33333", LastKnownCredits: 9},
		store.Account{ID: "a4", APIKey: "user_ddd44444", LastKnownCredits: 9},
	)
	p := New(st)
	seen := map[string]bool{}
	for i, prefix := range []string{"foo", "bar", "baz", "qux", "zog"} {
		got, err := p.Pick(PickOptions{
			ClientToken:    "tok",
			Model:          "m",
			MessagesPrefix: []byte(prefix),
		})
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		seen[got.ID] = true
	}
	if len(seen) < 2 {
		t.Errorf("all prefixes hashed to the same account: %v (want diversity)", seen)
	}
}

func TestPickExcludesTriedAccounts(t *testing.T) {
	st := newStoreWithAccounts(t,
		store.Account{ID: "a1", APIKey: "user_aaa11111", LastKnownCredits: 9},
		store.Account{ID: "a2", APIKey: "user_bbb22222", LastKnownCredits: 9},
	)
	p := New(st)
	first, _ := p.Pick(PickOptions{ClientToken: "t", Model: "m"})
	second, err := p.Pick(PickOptions{
		ClientToken: "t",
		Model:       "m",
		Exclude:     map[string]bool{first.ID: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.ID == first.ID {
		t.Fatalf("Exclude did not skip %q", first.ID)
	}
}

func TestPickSkipsPaused(t *testing.T) {
	st := newStoreWithAccounts(t,
		store.Account{ID: "a1", APIKey: "user_aaa11111", LastKnownCredits: 9, Paused: true},
		store.Account{ID: "a2", APIKey: "user_bbb22222", LastKnownCredits: 9},
	)
	p := New(st)
	got, err := p.Pick(PickOptions{ClientToken: "t", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "a2" {
		t.Errorf("got %q, want a2 (a1 is paused)", got.ID)
	}
}

func TestPickSkipsLowCredits(t *testing.T) {
	now := time.Now().UTC()
	st := newStoreWithAccounts(t,
		store.Account{ID: "low", APIKey: "user_aaa11111", LastKnownCredits: 0.1, LastKnownCreditsAt: now},
		store.Account{ID: "ok", APIKey: "user_bbb22222", LastKnownCredits: 5.0, LastKnownCreditsAt: now},
	)
	_ = st.Update(func(s *store.State) error {
		s.Settings.MinCreditsUSD = 0.5
		return nil
	})
	p := New(st)
	got, err := p.Pick(PickOptions{ClientToken: "t", Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "ok" {
		t.Errorf("got %q, want ok (low has 0.1 < threshold 0.5)", got.ID)
	}
}

func TestPickFiltersKnownZeroCredits(t *testing.T) {
	// Account with credits genuinely zero — billing succeeded recently
	// and reported $0. Should be filtered out under MinCreditsUSD>0.
	now := time.Now().UTC()
	st := newStoreWithAccounts(t,
		store.Account{ID: "zero", APIKey: "user_zero11111", LastKnownCredits: 0, LastKnownCreditsAt: now},
		store.Account{ID: "ok", APIKey: "user_okok22222", LastKnownCredits: 5.0, LastKnownCreditsAt: now},
	)
	_ = st.Update(func(s *store.State) error {
		s.Settings.MinCreditsUSD = 0.5
		return nil
	})
	p := New(st)
	for i := 0; i < 5; i++ {
		got, err := p.Pick(PickOptions{ClientToken: "tok" + string(rune('a'+i)), Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		if got.ID == "zero" {
			t.Errorf("iteration %d: known-zero account leaked into candidates", i)
		}
	}
}

func TestPickIncludesUnknownCredits(t *testing.T) {
	// Account just added with billing failing — LastKnownCreditsAt is
	// zero, LastKnownCredits is 0, but credits are unknown not known-
	// zero. Pool.Pick must NOT filter it: pool/sync.go will eventually
	// stamp a real value.
	st := newStoreWithAccounts(t,
		store.Account{ID: "unknown", APIKey: "user_unknown11"},
	)
	_ = st.Update(func(s *store.State) error {
		s.Settings.MinCreditsUSD = 0.5
		return nil
	})
	p := New(st)
	got, err := p.Pick(PickOptions{ClientToken: "tok", Model: "m"})
	if err != nil {
		t.Fatalf("Pick on a never-synced account returned err=%v; expected the account to be eligible until the syncer stamps a balance", err)
	}
	if got.ID != "unknown" {
		t.Errorf("got %q, want unknown", got.ID)
	}
}

func TestPickReturnsErrWhenAllUnhealthy(t *testing.T) {
	st := newStoreWithAccounts(t,
		store.Account{ID: "a1", APIKey: "user_aaa11111", LastKnownCredits: 9, Paused: true},
	)
	p := New(st)
	_, err := p.Pick(PickOptions{ClientToken: "t", Model: "m"})
	if err != ErrNoHealthyAccount {
		t.Errorf("err=%v, want ErrNoHealthyAccount", err)
	}
}

func TestMarkErrorEventuallyEjectsAccount(t *testing.T) {
	st := newStoreWithAccounts(t,
		store.Account{ID: "bad", APIKey: "user_aaa11111", LastKnownCredits: 9},
		store.Account{ID: "good", APIKey: "user_bbb22222", LastKnownCredits: 9},
	)
	_ = st.Update(func(s *store.State) error {
		s.Settings.MaxErrorRate5min = 0.5
		return nil
	})
	p := New(st)
	// Push the 'bad' account well past 50% error rate.
	for i := 0; i < 8; i++ {
		p.MarkError("bad")
	}
	p.MarkSuccess("bad") // 8 errs / 9 reqs ≈ 89%
	for i := 0; i < 10; i++ {
		p.MarkSuccess("good")
	}
	// Now Pick should never return 'bad' regardless of routing inputs.
	for i := 0; i < 12; i++ {
		got, err := p.Pick(PickOptions{
			ClientToken:    "t",
			Model:          "m",
			MessagesPrefix: []byte{byte(i)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.ID == "bad" {
			t.Fatalf("Pick returned ejected account on iter %d", i)
		}
	}
}

func TestRollingStatsWindowExpires(t *testing.T) {
	base := time.Now()
	rs := NewRollingStats(func() time.Time { return base })
	for i := 0; i < 10; i++ {
		rs.Record(false)
	}
	if rate := rs.ErrorRate(); rate != 1.0 {
		t.Fatalf("immediate ErrorRate=%v, want 1.0", rate)
	}
	// Advance past the window — all buckets retire.
	rs.clock = func() time.Time { return base.Add(rollingWindow + time.Second) }
	if rate := rs.ErrorRate(); rate != 0 {
		t.Errorf("after window ErrorRate=%v, want 0", rate)
	}
}
