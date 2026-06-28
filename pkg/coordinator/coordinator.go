// Copyright 2026 syzkaller project authors. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
)

const (
	// repairQueueSize is the buffer depth of the self-repair channel. Large
	// enough to absorb a burst of pool failures without dropping events, but
	// bounded to avoid unbounded memory growth.
	repairQueueSize = 64

	// canaryFraction is the proportion of pools selected for canary rollout.
	// 0.05 means 5 % of pools (at least one) receive the spec first.
	canaryFraction = 0.05
)

// GlobalCoordinator is the top-level orchestrator. It owns:
//   - A registry of WorkerPools (one per logical group of workers).
//   - The current active FormalSpec (stored lock-free via atomic.Pointer).
//   - A HealthMonitor that pings pools and fires self-repair requests.
//   - A repair goroutine that re-applies the spec to degraded pools.
//
// Spec deployments follow a staged-rollout pattern:
//  1. Validate the incoming spec.
//  2. Apply to a canary subset of pools concurrently; abort on failure.
//  3. Apply to the remaining pools concurrently.
//  4. Atomically update the active spec pointer.
type GlobalCoordinator struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.RWMutex
	pools    map[string]*WorkerPool
	prevSpec *FormalSpec // kept for rollback

	activeSpec specHolder

	monitor     *HealthMonitor
	repairQueue chan repairRequest

	// wg tracks the background goroutines so Stop() can wait for them.
	wg sync.WaitGroup
}

// New creates a GlobalCoordinator and starts its background goroutines
// (health monitoring and self-repair). Call Stop to shut down cleanly.
func New(ctx context.Context) *GlobalCoordinator {
	ctx, cancel := context.WithCancel(ctx)
	repairs := make(chan repairRequest, repairQueueSize)
	gc := &GlobalCoordinator{
		ctx:         ctx,
		cancel:      cancel,
		pools:       make(map[string]*WorkerPool),
		monitor:     newHealthMonitor(repairs),
		repairQueue: repairs,
	}
	gc.wg.Add(2)
	go func() {
		defer gc.wg.Done()
		gc.monitor.Run(ctx)
	}()
	go func() {
		defer gc.wg.Done()
		gc.runRepairLoop(ctx)
	}()
	return gc
}

// Stop cancels the coordinator context and waits for background goroutines to
// exit. It is safe to call multiple times.
func (gc *GlobalCoordinator) Stop() {
	gc.cancel()
	gc.wg.Wait()
}

// RegisterPool adds a new WorkerPool to the coordinator. The pool is also
// registered with the HealthMonitor. If a spec is already active, it is
// immediately applied to the new pool.
func (gc *GlobalCoordinator) RegisterPool(p *WorkerPool) error {
	if p == nil {
		return errors.New("pool must not be nil")
	}
	gc.mu.Lock()
	gc.pools[p.id] = p
	gc.mu.Unlock()

	gc.monitor.RegisterPool(p)

	// Catch the new pool up to the current spec, if any.
	if spec := gc.activeSpec.load(); spec != nil {
		if err := p.ApplySpec(spec); err != nil {
			return fmt.Errorf("could not apply current spec to new pool %q: %w", p.id, err)
		}
	}
	return nil
}

// DeploySpec validates the spec, applies it to a canary subset of pools, then
// rolls it out globally. On any canary failure the active spec is unchanged.
func (gc *GlobalCoordinator) DeploySpec(spec *FormalSpec) error {
	if err := spec.Validate(); err != nil {
		return fmt.Errorf("spec validation failed: %w", err)
	}

	// Ensure the new spec has a strictly greater version than the current one.
	if current := gc.activeSpec.load(); current != nil && spec.Version <= current.Version {
		return fmt.Errorf("spec version %d must be greater than current version %d",
			spec.Version, current.Version)
	}

	gc.mu.RLock()
	allPools := slices.Collect(maps.Values(gc.pools))
	gc.mu.RUnlock()

	if len(allPools) == 0 {
		// No pools yet: store the spec so newly registered pools pick it up.
		gc.mu.Lock()
		gc.prevSpec = gc.activeSpec.load()
		gc.mu.Unlock()
		gc.activeSpec.store(spec)
		return nil
	}

	canary, rest := splitCanary(allPools, canaryFraction)

	if err := gc.applyToPoolsConcurrently(canary, spec); err != nil {
		return fmt.Errorf("canary rollout failed: %w", err)
	}

	// Save previous spec for potential rollback before committing.
	gc.mu.Lock()
	gc.prevSpec = gc.activeSpec.load()
	gc.mu.Unlock()
	gc.activeSpec.store(spec)

	if err := gc.applyToPoolsConcurrently(rest, spec); err != nil {
		// Best-effort: record the error but do not roll back the spec pointer
		// because some full-fleet pools may already be on the new version.
		return fmt.Errorf("full-fleet rollout partially failed: %w", err)
	}
	return nil
}

// RollbackSpec reverts the active spec to the previous version, re-applying it
// to all pools. It returns an error if there is no previous spec.
func (gc *GlobalCoordinator) RollbackSpec() error {
	gc.mu.Lock()
	prev := gc.prevSpec
	gc.mu.Unlock()

	if prev == nil {
		return errors.New("no previous spec to roll back to")
	}

	gc.activeSpec.store(prev)

	gc.mu.RLock()
	pools := slices.Collect(maps.Values(gc.pools))
	gc.mu.RUnlock()

	return gc.applyToPoolsConcurrently(pools, prev)
}

// ActiveSpec returns the currently active FormalSpec, or nil if none has been
// deployed yet.
func (gc *GlobalCoordinator) ActiveSpec() *FormalSpec {
	return gc.activeSpec.load()
}

// applyToPoolsConcurrently fans out spec application across the given pools.
// It collects all errors and returns them joined.
func (gc *GlobalCoordinator) applyToPoolsConcurrently(pools []*WorkerPool, spec *FormalSpec) error {
	if len(pools) == 0 {
		return nil
	}
	type result struct {
		poolID string
		err    error
	}
	results := make(chan result, len(pools))
	for _, p := range pools {
		go func(pool *WorkerPool) {
			results <- result{poolID: pool.id, err: pool.ApplySpec(spec)}
		}(p)
	}

	var errs []error
	for range pools {
		r := <-results
		if r.err != nil {
			errs = append(errs, fmt.Errorf("pool %s: %w", r.poolID, r.err))
		}
	}
	return errors.Join(errs...)
}

// runRepairLoop processes repair requests emitted by the HealthMonitor. When a
// pool is flagged as unhealthy it re-applies the current active spec, giving
// workers the chance to self-repair. The goroutine exits when ctx is cancelled.
func (gc *GlobalCoordinator) runRepairLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-gc.repairQueue:
			gc.executeRepair(req)
		}
	}
}

// executeRepair re-applies the active spec to the degraded pool identified by
// req. If the pool is no longer registered or there is no active spec, it is a
// no-op.
func (gc *GlobalCoordinator) executeRepair(req repairRequest) {
	spec := gc.activeSpec.load()
	if spec == nil {
		return
	}
	gc.mu.RLock()
	p, ok := gc.pools[req.PoolID]
	gc.mu.RUnlock()
	if !ok {
		return
	}
	// Ignore errors here; the HealthMonitor will retry on the next cycle if
	// the pool remains unhealthy.
	_ = p.ApplySpec(spec)
}

// splitCanary partitions pools into a canary set and the remainder. The canary
// set contains at least one pool and at most ceil(fraction * total) pools.
func splitCanary(pools []*WorkerPool, fraction float64) (canary, rest []*WorkerPool) {
	n := int(float64(len(pools)) * fraction)
	if n < 1 {
		n = 1
	}
	if n > len(pools) {
		n = len(pools)
	}
	return pools[:n], pools[n:]
}
