package serverplugin

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rpcxio/libkv/store"
)

type fakeRedisRegisterStore struct {
	mu             sync.Mutex
	values         map[string][]byte
	putErr         error
	existsErrors   map[string]error
	deleteErrors   map[string]error
	closed         bool
	closeCalls     int
	blockOperation string
	blockKey       string
	blockStarted   chan struct{}
	blockRelease   chan struct{}
	blockUsed      bool
}

func newFakeRedisRegisterStore() *fakeRedisRegisterStore {
	return &fakeRedisRegisterStore{
		values:       make(map[string][]byte),
		existsErrors: make(map[string]error),
		deleteErrors: make(map[string]error),
	}
}

func (s *fakeRedisRegisterStore) Put(key string, value []byte, _ *store.WriteOptions) error {
	s.waitIfBlocked("Put", key)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.putErr != nil {
		return s.putErr
	}
	s.values[key] = append([]byte(nil), value...)
	return nil
}

func (s *fakeRedisRegisterStore) Get(key string) (*store.KVPair, error) {
	s.waitIfBlocked("Get", key)

	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[key]
	if !ok {
		return nil, store.ErrKeyNotFound
	}
	return &store.KVPair{Key: key, Value: append([]byte(nil), value...)}, nil
}

func (s *fakeRedisRegisterStore) Delete(key string) error {
	s.waitIfBlocked("Delete", key)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.deleteErrors[key]; err != nil {
		return err
	}
	delete(s.values, key)
	return nil
}

func (s *fakeRedisRegisterStore) Exists(key string) (bool, error) {
	s.waitIfBlocked("Exists", key)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.existsErrors[key]; err != nil {
		return false, err
	}
	_, ok := s.values[key]
	return ok, nil
}

func (s *fakeRedisRegisterStore) Watch(string, <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (s *fakeRedisRegisterStore) WatchTree(string, <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (s *fakeRedisRegisterStore) NewLock(string, *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

func (s *fakeRedisRegisterStore) List(string) ([]*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (s *fakeRedisRegisterStore) DeleteTree(string) error {
	return store.ErrCallNotSupported
}

func (s *fakeRedisRegisterStore) AtomicPut(string, []byte, *store.KVPair, *store.WriteOptions) (bool, *store.KVPair, error) {
	return false, nil, store.ErrCallNotSupported
}

func (s *fakeRedisRegisterStore) AtomicDelete(string, *store.KVPair) (bool, error) {
	return false, store.ErrCallNotSupported
}

func (s *fakeRedisRegisterStore) Close() {
	s.mu.Lock()
	s.closed = true
	s.closeCalls++
	s.mu.Unlock()
}

func (s *fakeRedisRegisterStore) setDeleteError(key string, err error) {
	s.mu.Lock()
	s.deleteErrors[key] = err
	s.mu.Unlock()
}

func (s *fakeRedisRegisterStore) setExistsError(key string, err error) {
	s.mu.Lock()
	s.existsErrors[key] = err
	s.mu.Unlock()
}

func (s *fakeRedisRegisterStore) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *fakeRedisRegisterStore) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}

func (s *fakeRedisRegisterStore) waitIfBlocked(operation, key string) {
	s.mu.Lock()
	var started chan struct{}
	var release chan struct{}
	if operation == s.blockOperation && key == s.blockKey && !s.blockUsed {
		s.blockUsed = true
		started = s.blockStarted
		release = s.blockRelease
	}
	s.mu.Unlock()

	if started != nil {
		close(started)
		<-release
	}
}

func (s *fakeRedisRegisterStore) blockNext(operation, key string) (<-chan struct{}, func()) {
	s.mu.Lock()
	started := make(chan struct{})
	release := make(chan struct{})
	s.blockOperation = operation
	s.blockKey = key
	s.blockStarted = started
	s.blockRelease = release
	s.blockUsed = false
	s.mu.Unlock()

	var once sync.Once
	return started, func() {
		once.Do(func() {
			close(release)
		})
	}
}

func (s *fakeRedisRegisterStore) blockNextGet(key string) (<-chan struct{}, func()) {
	return s.blockNext("Get", key)
}

func (s *fakeRedisRegisterStore) blockNextPut(key string) (<-chan struct{}, func()) {
	return s.blockNext("Put", key)
}

func (s *fakeRedisRegisterStore) blockNextExists(key string) (<-chan struct{}, func()) {
	return s.blockNext("Exists", key)
}

func (s *fakeRedisRegisterStore) blockNextDelete(key string) (<-chan struct{}, func()) {
	return s.blockNext("Delete", key)
}

func (s *fakeRedisRegisterStore) hasValue(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.values[key]
	return ok
}

func redisCallWithin(t *testing.T, timeout time.Duration, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		t.Fatal("operation did not finish")
		return nil
	}
}

func redisCallWithTimeout(t *testing.T, fn func() error) error {
	t.Helper()
	return redisCallWithin(t, time.Second, fn)
}

func waitForRedisStopRequest(t *testing.T, p *RedisRegisterPlugin) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if p.stopRequested.Load() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Stop() did not publish its request")
}

func TestRedisRegisterPluginRejectsEmptyBasePath(t *testing.T) {
	tests := []struct {
		name     string
		basePath string
		call     func(*RedisRegisterPlugin) error
	}{
		{name: "Start empty", call: func(p *RedisRegisterPlugin) error { return p.Start() }},
		{name: "Start whitespace", basePath: " \t ", call: func(p *RedisRegisterPlugin) error { return p.Start() }},
		{name: "Register empty", call: func(p *RedisRegisterPlugin) error { return p.Register("Arith", nil, "") }},
		{name: "Register whitespace", basePath: " \t ", call: func(p *RedisRegisterPlugin) error { return p.Register("Arith", nil, "") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &RedisRegisterPlugin{BasePath: tt.basePath, kv: newFakeRedisRegisterStore()}
			if err := tt.call(p); !errors.Is(err, errRedisBasePathEmpty) {
				t.Fatalf("error = %v, want %v", err, errRedisBasePathEmpty)
			}
		})
	}
}

func TestRedisRegisterPluginPreservesLeadingSlashBasePath(t *testing.T) {
	fake := newFakeRedisRegisterStore()
	p := &RedisRegisterPlugin{BasePath: "/rpcx", ServiceAddress: "tcp@127.0.0.1:8972", kv: fake}
	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := p.Register("Arith", nil, "state=active"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if p.BasePath != "/rpcx" {
		t.Fatalf("BasePath = %q, want leading slash preserved", p.BasePath)
	}
	if err := p.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestRedisRegisterPluginStopWithoutRefreshDoesNotBlock(t *testing.T) {
	fake := newFakeRedisRegisterStore()
	p := &RedisRegisterPlugin{BasePath: "rpcx", ServiceAddress: "tcp@127.0.0.1:8972", kv: fake}
	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := redisCallWithTimeout(t, p.Stop); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if !fake.isClosed() {
		t.Fatal("Stop() did not close the store")
	}
}

func TestRedisRegisterPluginStopBeforeStartAndRepeatedStop(t *testing.T) {
	fake := newFakeRedisRegisterStore()
	p := &RedisRegisterPlugin{BasePath: "rpcx", kv: fake}
	if err := redisCallWithTimeout(t, p.Stop); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}
	if err := redisCallWithTimeout(t, p.Stop); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
}

func TestRedisRegisterPluginStartFailureThenStopIsSafe(t *testing.T) {
	startErr := errors.New("start failed")
	fake := newFakeRedisRegisterStore()
	fake.putErr = startErr
	p := &RedisRegisterPlugin{BasePath: "rpcx", kv: fake}
	if err := p.Start(); !errors.Is(err, startErr) {
		t.Fatalf("Start() error = %v, want %v", err, startErr)
	}
	if err := redisCallWithTimeout(t, p.Stop); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if !fake.isClosed() {
		t.Fatal("failed Start() did not close the store")
	}
}

func TestRedisRegisterPluginStopRetriesFailedCleanup(t *testing.T) {
	fake := newFakeRedisRegisterStore()
	p := &RedisRegisterPlugin{
		BasePath:       "rpcx",
		ServiceAddress: "tcp@127.0.0.1:8972",
		UpdateInterval: time.Hour,
		kv:             fake,
	}
	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	for _, name := range []string{"Arith", "Greeter"} {
		if err := p.Register(name, nil, "state=active"); err != nil {
			t.Fatalf("Register(%q) error = %v", name, err)
		}
	}

	arithPath := "rpcx/Arith/tcp@127.0.0.1:8972"
	greeterPath := "rpcx/Greeter/tcp@127.0.0.1:8972"
	deleteErr := errors.New("delete Arith failed")
	existsErr := errors.New("check Greeter failed")
	fake.setDeleteError(arithPath, deleteErr)
	fake.setExistsError(greeterPath, existsErr)

	err := p.Stop()
	if !errors.Is(err, deleteErr) || !errors.Is(err, existsErr) {
		t.Fatalf("first Stop() error = %v, want both cleanup errors", err)
	}
	if fake.isClosed() {
		t.Fatal("first Stop() closed the store after cleanup failed")
	}
	if p.kv != fake {
		t.Fatal("first Stop() did not retain the store for a cleanup retry")
	}
	if !fake.hasValue(arithPath) || !fake.hasValue(greeterPath) {
		t.Fatal("first Stop() removed a node whose cleanup failed")
	}
	if err := p.Start(); !errors.Is(err, errRedisRegisterPluginStopped) {
		t.Fatalf("Start() after failed Stop error = %v, want %v", err, errRedisRegisterPluginStopped)
	}
	if err := p.Register("Other", nil, ""); !errors.Is(err, errRedisRegisterPluginStopped) {
		t.Fatalf("Register() after failed Stop error = %v, want %v", err, errRedisRegisterPluginStopped)
	}
	if err := p.Unregister("Arith"); err != nil {
		t.Fatalf("Unregister() after failed Stop error = %v, want nil", err)
	}

	fake.setDeleteError(arithPath, nil)
	fake.setExistsError(greeterPath, nil)
	if err := p.Stop(); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
	if fake.hasValue(arithPath) || fake.hasValue(greeterPath) {
		t.Fatal("second Stop() did not remove all registered nodes")
	}
	if !fake.isClosed() {
		t.Fatal("second Stop() did not close the store after cleanup succeeded")
	}
	if p.kv != nil {
		t.Fatal("second Stop() retained the store after cleanup succeeded")
	}
	if err := p.Stop(); err != nil {
		t.Fatalf("third Stop() error = %v", err)
	}
}

func TestRedisRegisterPluginUnregisterReturnsDuringRequestedStop(t *testing.T) {
	tests := []struct {
		name               string
		updateInterval     time.Duration
		block              func(*fakeRedisRegisterStore, string) (<-chan struct{}, func())
		waitBeforeStopping bool
	}{
		{name: "refresh Get", updateInterval: time.Millisecond, block: (*fakeRedisRegisterStore).blockNextGet, waitBeforeStopping: true},
		{name: "cleanup Exists", block: (*fakeRedisRegisterStore).blockNextExists},
		{name: "cleanup Delete", block: (*fakeRedisRegisterStore).blockNextDelete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeRedisRegisterStore()
			p := &RedisRegisterPlugin{
				BasePath:       "rpcx",
				ServiceAddress: "tcp@127.0.0.1:8972",
				UpdateInterval: tt.updateInterval,
				kv:             fake,
			}
			if err := p.Start(); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			if err := p.Register("Arith", nil, "state=active"); err != nil {
				t.Fatalf("Register() error = %v", err)
			}

			nodePath := "rpcx/Arith/tcp@127.0.0.1:8972"
			blocked, release := tt.block(fake, nodePath)
			defer release()
			if tt.waitBeforeStopping {
				select {
				case <-blocked:
				case <-time.After(time.Second):
					t.Fatal("refresh did not reach the blocked store operation")
				}
			}

			stopDone := make(chan error, 1)
			go func() {
				stopDone <- p.Stop()
			}()
			if !tt.waitBeforeStopping {
				select {
				case <-blocked:
				case <-time.After(time.Second):
					t.Fatal("Stop() did not reach the blocked store operation")
				}
			}
			waitForRedisStopRequest(t, p)

			if err := redisCallWithin(t, 100*time.Millisecond, func() error { return p.Unregister("Arith") }); err != nil {
				t.Fatalf("Unregister() during Stop error = %v, want nil", err)
			}
			if err := redisCallWithin(t, 100*time.Millisecond, p.Start); !errors.Is(err, errRedisRegisterPluginStopped) {
				t.Fatalf("Start() during Stop error = %v, want %v", err, errRedisRegisterPluginStopped)
			}
			if err := redisCallWithin(t, 100*time.Millisecond, func() error {
				return p.Register("Other", nil, "state=active")
			}); !errors.Is(err, errRedisRegisterPluginStopped) {
				t.Fatalf("Register() during Stop error = %v, want %v", err, errRedisRegisterPluginStopped)
			}

			release()
			select {
			case err := <-stopDone:
				if err != nil {
					t.Fatalf("Stop() error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Stop() did not finish after the store operation was released")
			}
			if fake.hasValue(nodePath) {
				t.Fatal("Stop() did not remove the registered node")
			}
			if !fake.isClosed() {
				t.Fatal("Stop() did not close the store")
			}
		})
	}
}

func TestRedisRegisterPluginConcurrentStopClosesOnce(t *testing.T) {
	fake := newFakeRedisRegisterStore()
	p := &RedisRegisterPlugin{
		BasePath:       "rpcx",
		ServiceAddress: "tcp@127.0.0.1:8972",
		kv:             fake,
	}
	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := p.Register("Arith", nil, "state=active"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	nodePath := "rpcx/Arith/tcp@127.0.0.1:8972"
	blocked, release := fake.blockNextExists(nodePath)
	defer release()
	stopDone := make(chan error, 2)
	go func() { stopDone <- p.Stop() }()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("first Stop() did not reach the blocked Exists")
	}
	go func() { stopDone <- p.Stop() }()
	select {
	case err := <-stopDone:
		t.Fatalf("Stop() returned before the blocked cleanup was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	release()
	for i := 0; i < 2; i++ {
		select {
		case err := <-stopDone:
			if err != nil {
				t.Fatalf("Stop() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent Stop() did not finish")
		}
	}
	if got := fake.closeCount(); got != 1 {
		t.Fatalf("store Close() calls = %d, want 1", got)
	}
}

func TestRedisRegisterPluginStopTimeoutIsBoundedAndRetryable(t *testing.T) {
	tests := []struct {
		name           string
		updateInterval time.Duration
		block          func(*fakeRedisRegisterStore, string) (<-chan struct{}, func())
		waitForBlock   bool
	}{
		{name: "refresh shutdown", updateInterval: time.Millisecond, block: (*fakeRedisRegisterStore).blockNextGet, waitForBlock: true},
		{name: "cleanup exists", block: (*fakeRedisRegisterStore).blockNextExists},
		{name: "cleanup delete", block: (*fakeRedisRegisterStore).blockNextDelete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeRedisRegisterStore()
			p := &RedisRegisterPlugin{
				BasePath:       "rpcx",
				ServiceAddress: "tcp@127.0.0.1:8972",
				UpdateInterval: tt.updateInterval,
				StopTimeout:    25 * time.Millisecond,
				kv:             fake,
			}
			if err := p.Start(); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			if err := p.Register("Arith", nil, "state=active"); err != nil {
				t.Fatalf("Register() error = %v", err)
			}

			nodePath := "rpcx/Arith/tcp@127.0.0.1:8972"
			blocked, release := tt.block(fake, nodePath)
			defer release()
			if tt.waitForBlock {
				select {
				case <-blocked:
				case <-time.After(time.Second):
					t.Fatal("refresh did not reach the blocked store operation")
				}
			}

			start := time.Now()
			err := p.Stop()
			if !errors.Is(err, ErrRedisRegisterPluginStopTimeout) {
				t.Fatalf("Stop() error = %v, want %v", err, ErrRedisRegisterPluginStopTimeout)
			}
			if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
				t.Fatalf("Stop() exceeded its budget: %v", elapsed)
			}
			if !tt.waitForBlock {
				select {
				case <-blocked:
				case <-time.After(time.Second):
					t.Fatal("cleanup did not reach the blocked store operation")
				}
			}
			if fake.isClosed() {
				t.Fatal("timed-out cleanup closed the store before it completed")
			}

			release()
			if err := redisCallWithin(t, time.Second, p.Stop); err != nil {
				t.Fatalf("Stop() retry error = %v", err)
			}
			if !fake.isClosed() {
				t.Fatal("completed cleanup did not close the store")
			}
		})
	}
}

func TestRedisRegisterPluginStopBudgetIncludesLifecycleContention(t *testing.T) {
	tests := []struct {
		name      string
		prepare   func(*testing.T, *RedisRegisterPlugin)
		block     func(*fakeRedisRegisterStore, string) (<-chan struct{}, func())
		operation func(*RedisRegisterPlugin) error
	}{
		{
			name:      "register put",
			block:     (*fakeRedisRegisterStore).blockNextPut,
			operation: func(p *RedisRegisterPlugin) error { return p.Register("Arith", nil, "state=active") },
		},
		{
			name: "unregister delete",
			prepare: func(t *testing.T, p *RedisRegisterPlugin) {
				t.Helper()
				if err := p.Register("Arith", nil, "state=active"); err != nil {
					t.Fatalf("Register() error = %v", err)
				}
			},
			block:     (*fakeRedisRegisterStore).blockNextDelete,
			operation: func(p *RedisRegisterPlugin) error { return p.Unregister("Arith") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeRedisRegisterStore()
			p := &RedisRegisterPlugin{
				BasePath:       "rpcx",
				ServiceAddress: "tcp@127.0.0.1:8972",
				StopTimeout:    25 * time.Millisecond,
				kv:             fake,
			}
			if err := p.Start(); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			if tt.prepare != nil {
				tt.prepare(t, p)
			}

			nodePath := "rpcx/Arith/tcp@127.0.0.1:8972"
			blocked, release := tt.block(fake, nodePath)
			defer release()
			operationDone := make(chan error, 1)
			go func() { operationDone <- tt.operation(p) }()
			select {
			case <-blocked:
			case <-time.After(time.Second):
				t.Fatal("lifecycle operation did not reach the blocked store call")
			}

			start := time.Now()
			err := p.Stop()
			if !errors.Is(err, ErrRedisRegisterPluginStopTimeout) {
				t.Fatalf("Stop() error = %v, want %v", err, ErrRedisRegisterPluginStopTimeout)
			}
			if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
				t.Fatalf("Stop() lifecycle-lock wait exceeded its budget: %v", elapsed)
			}

			release()
			select {
			case err := <-operationDone:
				if err != nil {
					t.Fatalf("blocked lifecycle operation error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("blocked lifecycle operation did not finish")
			}
			if err := redisCallWithin(t, time.Second, p.Stop); err != nil {
				t.Fatalf("Stop() retry error = %v", err)
			}
			if !fake.isClosed() {
				t.Fatal("Stop() retry did not close the store")
			}
		})
	}
}

func TestRedisRegisterPluginConcurrentRegisterAndRefresh(t *testing.T) {
	fake := newFakeRedisRegisterStore()
	p := &RedisRegisterPlugin{
		BasePath:       "rpcx",
		ServiceAddress: "tcp@127.0.0.1:8972",
		UpdateInterval: time.Millisecond,
		kv:             fake,
	}
	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	errCh := make(chan error, 40)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for service := 0; service < 10; service++ {
				name := fmt.Sprintf("service-%d-%d", worker, service)
				if err := p.Register(name, nil, "state=active"); err != nil {
					errCh <- err
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("Register() error = %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := p.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestRedisRegisterPluginUnregisterWinsAgainstInFlightRefresh(t *testing.T) {
	fake := newFakeRedisRegisterStore()
	p := &RedisRegisterPlugin{
		BasePath:       "rpcx",
		ServiceAddress: "tcp@127.0.0.1:8972",
		UpdateInterval: time.Millisecond,
		kv:             fake,
	}
	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := p.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	}()
	if err := p.Register("Arith", nil, "state=active"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	nodePath := "rpcx/Arith/tcp@127.0.0.1:8972"
	refreshStarted, releaseRefresh := fake.blockNextGet(nodePath)
	defer releaseRefresh()
	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		t.Fatal("refresh did not reach the blocked Get")
	}

	unregisterDone := make(chan error, 1)
	go func() {
		unregisterDone <- p.Unregister("Arith")
	}()
	select {
	case err := <-unregisterDone:
		t.Fatalf("Unregister() returned before the in-flight refresh completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	releaseRefresh()
	select {
	case err := <-unregisterDone:
		if err != nil {
			t.Fatalf("Unregister() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Unregister() did not finish")
	}

	time.Sleep(5 * time.Millisecond)
	if fake.hasValue(nodePath) {
		t.Fatal("refresh recreated the service after Unregister() returned")
	}
}
