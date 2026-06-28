// Copyright 2026 syzkaller project authors. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

// Package coordinator implements a GlobalCoordinator that distributes formal
// specifications to pools of worker processes (e.g. Rust-based executors) via
// goroutine-based concurrency, providing self-repair and auto-scheduling at
// scale. The design mirrors syzkaller's existing patterns (context propagation,
// sync.RWMutex, channel-based signalling) while generalising them for
// multi-pool, multi-worker scenarios.
package coordinator

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync/atomic"
)

// FormalSpec is a versioned, content-addressed specification that the
// GlobalCoordinator deploys to every worker pool. Workers record which version
// they have applied; mismatches trigger the self-repair path.
type FormalSpec struct {
	// Version is a monotonically increasing counter. A zero value is invalid.
	Version uint64

	// Data is the raw specification payload sent to workers.
	Data []byte

	// checksum is the SHA-256 digest of Data, populated by NewFormalSpec.
	checksum [sha256.Size]byte
}

// NewFormalSpec creates a new FormalSpec with the given version and payload.
// It returns an error if the spec fails basic validation.
func NewFormalSpec(version uint64, data []byte) (*FormalSpec, error) {
	s := &FormalSpec{
		Version:  version,
		Data:     data,
		checksum: sha256.Sum256(data),
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// Validate performs integrity checks on the spec: it recomputes the SHA-256
// digest and compares it with the stored checksum, and it rejects the zero
// version.
func (s *FormalSpec) Validate() error {
	if s.Version == 0 {
		return errors.New("spec version must be non-zero")
	}
	if got := sha256.Sum256(s.Data); got != s.checksum {
		return fmt.Errorf("spec checksum mismatch: stored %x, computed %x", s.checksum, got)
	}
	return nil
}

// Checksum returns the SHA-256 digest of the spec payload.
func (s *FormalSpec) Checksum() [sha256.Size]byte {
	return s.checksum
}

// specHolder provides lock-free storage of the current active FormalSpec.
// atomic.Pointer is used so reads never need a mutex.
type specHolder struct {
	ptr atomic.Pointer[FormalSpec]
}

func (h *specHolder) load() *FormalSpec {
	return h.ptr.Load()
}

func (h *specHolder) store(s *FormalSpec) {
	h.ptr.Store(s)
}
