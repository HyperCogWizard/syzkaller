// Copyright 2026 syzkaller project authors. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package coordinator

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// WorkerConn is the abstract interface through which the coordinator
// communicates with a single remote worker process. Implementations may use
// Unix sockets, gRPC, FlatBuffers over TCP, or any other transport — the
// coordinator does not depend on the wire format.
type WorkerConn interface {
	// ID returns a stable identifier for this worker (e.g. hostname:port).
	ID() string

	// ApplySpec delivers a formal specification to the worker. The worker is
	// expected to hot-reload its behaviour without restarting.
	ApplySpec(spec *FormalSpec) error

	// Ping checks liveness of the worker. It is called by the HealthMonitor.
	Ping() error
}

// WorkerPool manages a named group of WorkerConn instances. Each pool tracks
// which spec version its workers have applied and exposes an ApplySpec method
// used by the GlobalCoordinator during staged rollouts.
type WorkerPool struct {
	id           string
	mu           sync.Mutex
	workers      []WorkerConn
	appliedVer   atomic.Uint64 // highest successfully applied spec version
	tasksPending atomic.Int64  // inflight task count for back-pressure metrics
}

// newWorkerPool creates an empty WorkerPool with the given identifier.
func newWorkerPool(id string) *WorkerPool {
	return &WorkerPool{id: id}
}

// Add appends a worker to the pool. It is safe to call concurrently.
func (p *WorkerPool) Add(w WorkerConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workers = append(p.workers, w)
}

// Remove removes the worker with the given ID from the pool. It returns an
// error if no such worker is registered.
func (p *WorkerPool) Remove(workerID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, w := range p.workers {
		if w.ID() == workerID {
			last := len(p.workers) - 1
			p.workers[i] = p.workers[last]
			p.workers[last] = nil
			p.workers = p.workers[:last]
			return nil
		}
	}
	return fmt.Errorf("worker %q not found in pool %q", workerID, p.id)
}

// ApplySpec fans the specification out to every worker in the pool
// concurrently. It collects all errors but does not abort on the first
// failure, so that a single unhealthy worker cannot block the rest. The
// applied version counter is only advanced when at least one worker succeeds.
func (p *WorkerPool) ApplySpec(spec *FormalSpec) error {
	p.mu.Lock()
	workers := make([]WorkerConn, len(p.workers))
	copy(workers, p.workers)
	p.mu.Unlock()

	if len(workers) == 0 {
		return errors.New("pool has no workers")
	}

	type result struct {
		id  string
		err error
	}

	results := make(chan result, len(workers))
	for _, w := range workers {
		go func(wc WorkerConn) {
			results <- result{id: wc.ID(), err: wc.ApplySpec(spec)}
		}(w)
	}

	var errs []error
	okCount := 0
	for range workers {
		r := <-results
		if r.err != nil {
			errs = append(errs, fmt.Errorf("worker %s: %w", r.id, r.err))
		} else {
			okCount++
		}
	}

	if okCount > 0 {
		p.appliedVer.Store(spec.Version)
	}

	return errors.Join(errs...)
}

// AppliedVersion returns the highest spec version that has been successfully
// applied to at least one worker in this pool.
func (p *WorkerPool) AppliedVersion() uint64 {
	return p.appliedVer.Load()
}

// Len returns the current number of workers in the pool.
func (p *WorkerPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

// healthyCount returns the number of workers that respond to a Ping.
// It is called from the HealthMonitor goroutine.
func (p *WorkerPool) healthyCount() int {
	p.mu.Lock()
	workers := make([]WorkerConn, len(p.workers))
	copy(workers, p.workers)
	p.mu.Unlock()

	type ok struct{ alive bool }
	results := make(chan ok, len(workers))
	for _, w := range workers {
		go func(wc WorkerConn) {
			results <- ok{alive: wc.Ping() == nil}
		}(w)
	}

	count := 0
	for range workers {
		if (<-results).alive {
			count++
		}
	}
	return count
}
