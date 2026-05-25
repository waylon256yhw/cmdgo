package store

import "time"

// AppendTrafficLog adds entry to the rolling traffic log and trims to
// Settings.TrafficLogMax (oldest entries dropped first). When TS is
// zero it's filled with time.Now().UTC() inside the lock so the
// timestamp matches the order rows actually land on disk.
func (s *Store) AppendTrafficLog(entry TrafficEntry) error {
	return s.Update(func(st *State) error {
		if entry.TS.IsZero() {
			entry.TS = time.Now().UTC()
		}
		st.TrafficLog = append(st.TrafficLog, entry)
		max := st.Settings.TrafficLogMax
		if max <= 0 {
			max = 500
		}
		if len(st.TrafficLog) > max {
			st.TrafficLog = append([]TrafficEntry(nil), st.TrafficLog[len(st.TrafficLog)-max:]...)
		}
		return nil
	})
}

// TrafficLog returns a copy of the rolling log, newest first.
func (s *Store) TrafficLog(limit int) []TrafficEntry {
	snap := s.Snapshot()
	rows := snap.TrafficLog
	if limit > 0 && len(rows) > limit {
		rows = rows[len(rows)-limit:]
	}
	out := make([]TrafficEntry, len(rows))
	for i := range rows {
		out[i] = rows[len(rows)-1-i]
	}
	return out
}

// TouchAccountLastUsed bumps LastUsedAt on the account with the given
// ID. Used by the proxy adapter after a successful request. No-op
// if the account no longer exists.
func (s *Store) TouchAccountLastUsed(id string) error {
	now := time.Now().UTC()
	return s.Update(func(st *State) error {
		for i := range st.Accounts {
			if st.Accounts[i].ID == id {
				st.Accounts[i].LastUsedAt = now
				return nil
			}
		}
		return nil
	})
}

// SetAccountCredits updates LastKnownCredits on the account. Used by
// the 60s credit sync poller.
func (s *Store) SetAccountCredits(id string, credits float64) error {
	now := time.Now().UTC()
	return s.Update(func(st *State) error {
		for i := range st.Accounts {
			if st.Accounts[i].ID == id {
				st.Accounts[i].LastKnownCredits = credits
				st.Accounts[i].LastKnownCreditsAt = now
				return nil
			}
		}
		return nil
	})
}

// SetAccountPaused toggles the paused flag. Returns ErrAccountNotFound
// if id is unknown.
func (s *Store) SetAccountPaused(id string, paused bool) error {
	return s.Update(func(st *State) error {
		for i := range st.Accounts {
			if st.Accounts[i].ID == id {
				st.Accounts[i].Paused = paused
				return nil
			}
		}
		return ErrAccountNotFound
	})
}

// DeleteAccount removes the account with the given ID.
func (s *Store) DeleteAccount(id string) error {
	return s.Update(func(st *State) error {
		for i := range st.Accounts {
			if st.Accounts[i].ID == id {
				st.Accounts = append(st.Accounts[:i], st.Accounts[i+1:]...)
				return nil
			}
		}
		return ErrAccountNotFound
	})
}
