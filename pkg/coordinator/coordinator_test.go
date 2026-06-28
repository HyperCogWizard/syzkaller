// Copyright 2026 syzkaller project authors. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package coordinator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeWorker is an in-process WorkerConn used in tests. It records the spec
// version it received and can be configured to return errors.
type fakeWorker struct {
	id         string
	appliedVer atomic.Uint64
	pingErr    error
	applyErr   error
	mu         sync.Mutex
	applyCount int
}

func newFakeWorker(id string) *fakeWorker {
	return &fakeWorker{id: id}
}

func (w *fakeWorker) ID() string { return w.id }

func (w *fakeWorker) ApplySpec(spec *FormalSpec) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.applyErr != nil {
		return w.applyErr
	}
	w.appliedVer.Store(spec.Version)
	w.applyCount++
	return nil
}

func (w *fakeWorker) Ping() error {
	return w.pingErr
}

// --- FormalSpec tests ---

func TestNewFormalSpec_valid(t *testing.T) {
	spec, err := NewFormalSpec(1, []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), spec.Version)
	require.Equal(t, []byte("hello"), spec.Data)
}

func TestNewFormalSpec_zeroVersion(t *testing.T) {
	_, err := NewFormalSpec(0, []byte("data"))
	require.Error(t, err)
}

func TestFormalSpec_Validate_checksumTamper(t *testing.T) {
	spec, err := NewFormalSpec(1, []byte("original"))
	require.NoError(t, err)
	// Tamper with the data after creation.
	spec.Data = []byte("tampered")
	err = spec.Validate()
	require.Error(t, err)
}

// --- WorkerPool tests ---

func TestWorkerPool_ApplySpec(t *testing.T) {
	p := newWorkerPool("pool-a")
	w1 := newFakeWorker("w1")
	w2 := newFakeWorker("w2")
	p.Add(w1)
	p.Add(w2)

	spec, err := NewFormalSpec(3, []byte("v3"))
	require.NoError(t, err)

	err = p.ApplySpec(spec)
	require.NoError(t, err)
	require.Equal(t, uint64(3), p.AppliedVersion())
	require.Equal(t, uint64(3), w1.appliedVer.Load())
	require.Equal(t, uint64(3), w2.appliedVer.Load())
}

func TestWorkerPool_ApplySpec_empty(t *testing.T) {
	p := newWorkerPool("empty")
	spec, _ := NewFormalSpec(1, []byte("x"))
	err := p.ApplySpec(spec)
	require.Error(t, err)
}

func TestWorkerPool_ApplySpec_partialFailure(t *testing.T) {
	p := newWorkerPool("pool-b")
	good := newFakeWorker("good")
	bad := newFakeWorker("bad")
	bad.applyErr = errors.New("injected error")
	p.Add(good)
	p.Add(bad)

	spec, _ := NewFormalSpec(5, []byte("payload"))
	err := p.ApplySpec(spec)
	// Error is returned…
	require.Error(t, err)
	// …but the applied version is still advanced because at least one worker succeeded.
	require.Equal(t, uint64(5), p.AppliedVersion())
	require.Equal(t, uint64(5), good.appliedVer.Load())
}

func TestWorkerPool_Remove(t *testing.T) {
	p := newWorkerPool("pool-c")
	w := newFakeWorker("w1")
	p.Add(w)
	require.Equal(t, 1, p.Len())

	err := p.Remove("w1")
	require.NoError(t, err)
	require.Equal(t, 0, p.Len())
}

func TestWorkerPool_Remove_notFound(t *testing.T) {
	p := newWorkerPool("pool-d")
	err := p.Remove("missing")
	require.Error(t, err)
}

// --- HealthMonitor tests ---

func TestHealthMonitor_checkPool_allHealthy(t *testing.T) {
	repairs := make(chan repairRequest, 10)
	m := newHealthMonitor(repairs)

	p := newWorkerPool("p1")
	p.Add(newFakeWorker("w1"))
	p.Add(newFakeWorker("w2"))
	m.RegisterPool(p)

	h := m.checkPool(p)
	require.True(t, h.Healthy)
	require.Equal(t, 2, h.TotalWorkers)
	require.Equal(t, 2, h.HealthyWorkers)
}

func TestHealthMonitor_checkPool_majority_unhealthy(t *testing.T) {
	repairs := make(chan repairRequest, 10)
	m := newHealthMonitor(repairs)

	p := newWorkerPool("p2")
	dead1 := newFakeWorker("d1")
	dead1.pingErr = errors.New("unreachable")
	dead2 := newFakeWorker("d2")
	dead2.pingErr = errors.New("unreachable")
	alive := newFakeWorker("a1")
	p.Add(dead1)
	p.Add(dead2)
	p.Add(alive)
	m.RegisterPool(p)

	h := m.checkPool(p)
	// Only 1 of 3 workers is alive → below 0.5 threshold.
	require.False(t, h.Healthy)
}

func TestHealthMonitor_checkAll_emitsRepair(t *testing.T) {
	repairs := make(chan repairRequest, 10)
	m := newHealthMonitor(repairs)

	p := newWorkerPool("sick")
	dead := newFakeWorker("dead")
	dead.pingErr = errors.New("dead")
	p.Add(dead)
	m.RegisterPool(p)

	m.checkAll()

	select {
	case req := <-repairs:
		require.Equal(t, "sick", req.PoolID)
		require.False(t, req.Health.Healthy)
	case <-time.After(time.Second):
		t.Fatal("expected repair request, got none")
	}
}

// --- GlobalCoordinator tests ---

func TestCoordinator_DeploySpec_noPoolsYet(t *testing.T) {
	gc := New(context.Background())
	defer gc.Stop()

	spec, _ := NewFormalSpec(1, []byte("initial"))
	err := gc.DeploySpec(spec)
	require.NoError(t, err)
	require.Equal(t, spec, gc.ActiveSpec())
}

func TestCoordinator_DeploySpec_singlePool(t *testing.T) {
	gc := New(context.Background())
	defer gc.Stop()

	p := newWorkerPool("main")
	w := newFakeWorker("w1")
	p.Add(w)
	require.NoError(t, gc.RegisterPool(p))

	spec, _ := NewFormalSpec(2, []byte("spec-v2"))
	err := gc.DeploySpec(spec)
	require.NoError(t, err)
	require.Equal(t, uint64(2), w.appliedVer.Load())
	require.Equal(t, spec, gc.ActiveSpec())
}

func TestCoordinator_DeploySpec_staleVersion(t *testing.T) {
	gc := New(context.Background())
	defer gc.Stop()

	s1, _ := NewFormalSpec(10, []byte("v10"))
	require.NoError(t, gc.DeploySpec(s1))

	// Attempt to deploy a lower version.
	s2, _ := NewFormalSpec(9, []byte("v9"))
	err := gc.DeploySpec(s2)
	require.Error(t, err)
	// Active spec must remain v10.
	require.Equal(t, uint64(10), gc.ActiveSpec().Version)
}

func TestCoordinator_DeploySpec_invalidSpec(t *testing.T) {
	gc := New(context.Background())
	defer gc.Stop()

	bad := &FormalSpec{Version: 0, Data: []byte("x")}
	err := gc.DeploySpec(bad)
	require.Error(t, err)
}

func TestCoordinator_RollbackSpec(t *testing.T) {
	gc := New(context.Background())
	defer gc.Stop()

	p := newWorkerPool("pool")
	w := newFakeWorker("w1")
	p.Add(w)
	require.NoError(t, gc.RegisterPool(p))

	s1, _ := NewFormalSpec(1, []byte("v1"))
	s2, _ := NewFormalSpec(2, []byte("v2"))
	require.NoError(t, gc.DeploySpec(s1))
	require.NoError(t, gc.DeploySpec(s2))

	err := gc.RollbackSpec()
	require.NoError(t, err)
	require.Equal(t, uint64(1), gc.ActiveSpec().Version)
	require.Equal(t, uint64(1), w.appliedVer.Load())
}

func TestCoordinator_RollbackSpec_noPrevious(t *testing.T) {
	gc := New(context.Background())
	defer gc.Stop()
	require.Error(t, gc.RollbackSpec())
}

func TestCoordinator_RegisterPool_catchesUpSpec(t *testing.T) {
	gc := New(context.Background())
	defer gc.Stop()

	// Deploy a spec before the pool is registered.
	spec, _ := NewFormalSpec(7, []byte("catch-up"))
	require.NoError(t, gc.DeploySpec(spec))

	// Now register a pool: it should receive the spec immediately.
	p := newWorkerPool("late")
	w := newFakeWorker("w-late")
	p.Add(w)
	require.NoError(t, gc.RegisterPool(p))
	require.Equal(t, uint64(7), w.appliedVer.Load())
}

func TestCoordinator_SelfRepair_reappliesSpec(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gc := New(ctx)
	defer gc.Stop()

	p := newWorkerPool("recovering")
	w := newFakeWorker("w1")
	p.Add(w)
	require.NoError(t, gc.RegisterPool(p))

	spec, _ := NewFormalSpec(3, []byte("spec"))
	require.NoError(t, gc.DeploySpec(spec))

	// Simulate a repair request arriving for the pool.
	req := repairRequest{PoolID: "recovering", Health: PoolHealth{Healthy: false}}
	gc.repairQueue <- req

	// Give the repair goroutine time to process.
	require.Eventually(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.applyCount >= 2
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCoordinator_MultiplePoolsConcurrentDeploy(t *testing.T) {
	gc := New(context.Background())
	defer gc.Stop()

	const numPools = 10
	workers := make([]*fakeWorker, numPools)
	for i := range numPools {
		p := newWorkerPool(time.Now().String() + string(rune('A'+i)))
		w := newFakeWorker(p.id + "-w")
		workers[i] = w
		p.Add(w)
		require.NoError(t, gc.RegisterPool(p))
	}

	spec, _ := NewFormalSpec(42, []byte("mass-deploy"))
	err := gc.DeploySpec(spec)
	require.NoError(t, err)

	for i, w := range workers {
		require.Equal(t, uint64(42), w.appliedVer.Load(), "pool %d worker not updated", i)
	}
}

func TestSplitCanary(t *testing.T) {
	pools := make([]*WorkerPool, 20)
	for i := range pools {
		pools[i] = newWorkerPool("p")
	}
	canary, rest := splitCanary(pools, 0.05)
	require.Equal(t, 1, len(canary))
	require.Equal(t, 19, len(rest))

	// With 100 pools, 5 % = 5 canary pools.
	big := make([]*WorkerPool, 100)
	for i := range big {
		big[i] = newWorkerPool("p")
	}
	canary2, rest2 := splitCanary(big, 0.05)
	require.Equal(t, 5, len(canary2))
	require.Equal(t, 95, len(rest2))
}

func TestSplitCanary_singlePool(t *testing.T) {
	pools := []*WorkerPool{newWorkerPool("only")}
	canary, rest := splitCanary(pools, 0.05)
	require.Equal(t, 1, len(canary))
	require.Equal(t, 0, len(rest))
}
