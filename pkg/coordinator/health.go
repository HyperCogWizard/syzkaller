// Copyright 2026 syzkaller project authors. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package coordinator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// defaultCheckInterval is how often the HealthMonitor polls pools.
	defaultCheckInterval = 5 * time.Second

	// defaultUnhealthyThreshold is the minimum fraction of workers in a pool
	// that must be healthy for the pool to be considered healthy. 0.5 means
	// that a majority must be reachable.
	defaultUnhealthyThreshold = 0.5
)

// PoolHealth describes the health status of a single WorkerPool at a point in
// time.
type PoolHealth struct {
	PoolID         string
	TotalWorkers   int
	HealthyWorkers int
	Healthy        bool
}

// HealthMonitor runs a background goroutine that periodically pings every pool
// and emits repair requests when pools become unhealthy. Callers may also
// register pools and record out-of-band heartbeats from workers.
type HealthMonitor struct {
	mu            sync.RWMutex
	pools         map[string]*WorkerPool
	heartbeats    map[string]time.Time // poolID → last heartbeat
	checkInterval time.Duration
	threshold     float64 // fraction of workers that must be healthy

	// repairs is sent to whenever a pool needs attention. The channel is
	// buffered so that the monitoring goroutine never blocks on a slow
	// coordinator.
	repairs chan repairRequest

	// unhealthyCount tracks the total number of pools currently flagged unhealthy.
	unhealthyCount atomic.Int64
}

// repairRequest is sent from the HealthMonitor to the GlobalCoordinator's
// self-repair goroutine.
type repairRequest struct {
	PoolID string
	Health PoolHealth
}

// newHealthMonitor creates a HealthMonitor with default check interval and
// threshold. The returned monitor must be started with Run.
func newHealthMonitor(repairs chan repairRequest) *HealthMonitor {
	return &HealthMonitor{
		pools:         make(map[string]*WorkerPool),
		heartbeats:    make(map[string]time.Time),
		checkInterval: defaultCheckInterval,
		threshold:     defaultUnhealthyThreshold,
		repairs:       repairs,
	}
}

// RegisterPool adds a pool to the set monitored by this HealthMonitor.
func (m *HealthMonitor) RegisterPool(p *WorkerPool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pools[p.id] = p
}

// RecordHeartbeat records a liveness signal from a pool, typically delivered
// as a side-effect of any successful RPC. This is used alongside active Ping
// checks to avoid false positives when the check interval is short.
func (m *HealthMonitor) RecordHeartbeat(poolID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeats[poolID] = time.Now()
}

// Run starts the health-check loop. It returns when ctx is cancelled.
func (m *HealthMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkAll()
		}
	}
}

// checkAll iterates over all registered pools and emits repair requests for
// unhealthy ones.
func (m *HealthMonitor) checkAll() {
	m.mu.RLock()
	pools := make([]*WorkerPool, 0, len(m.pools))
	for _, p := range m.pools {
		pools = append(pools, p)
	}
	m.mu.RUnlock()

	unhealthy := int64(0)
	for _, p := range pools {
		h := m.checkPool(p)
		if !h.Healthy {
			unhealthy++
			// Non-blocking send: if the repair queue is full we skip rather
			// than block the monitor goroutine.
			select {
			case m.repairs <- repairRequest{PoolID: p.id, Health: h}:
			default:
			}
		}
	}
	m.unhealthyCount.Store(unhealthy)
}

// checkPool evaluates the health of a single pool and returns a PoolHealth.
func (m *HealthMonitor) checkPool(p *WorkerPool) PoolHealth {
	total := p.Len()
	healthy := p.healthyCount()

	isHealthy := total > 0 && float64(healthy)/float64(total) >= m.threshold
	return PoolHealth{
		PoolID:         p.id,
		TotalWorkers:   total,
		HealthyWorkers: healthy,
		Healthy:        isHealthy,
	}
}

// UnhealthyCount returns the number of pools that were unhealthy the last time
// checkAll ran.
func (m *HealthMonitor) UnhealthyCount() int64 {
	return m.unhealthyCount.Load()
}

// PoolStatus returns the current health snapshot for a single pool.
func (m *HealthMonitor) PoolStatus(poolID string) (PoolHealth, bool) {
	m.mu.RLock()
	p, ok := m.pools[poolID]
	m.mu.RUnlock()
	if !ok {
		return PoolHealth{}, false
	}
	return m.checkPool(p), true
}
