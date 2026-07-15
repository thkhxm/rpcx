package serverplugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/rpcxio/libkv"
	"github.com/rpcxio/libkv/store"
	"github.com/rpcxio/libkv/store/redis"
	"github.com/thkhxm/rpcx/v2/log"
)

func init() {
	redis.Register()
}

// RedisRegisterPlugin implements redis registry.
type RedisRegisterPlugin struct {
	// service address, for example, tcp@127.0.0.1:8972, quic@127.0.0.1:1234
	ServiceAddress string
	// redis addresses
	RedisServers []string
	// base path for rpcx server, for example com/example/rpcx
	BasePath string
	Metrics  metrics.Registry
	// Registered services
	Services       []string
	stateLock      sync.RWMutex
	metas          map[string]string
	UpdateInterval time.Duration
	// StopTimeout bounds how long each Stop call waits for lifecycle contention,
	// refresh shutdown, and registry cleanup. Zero uses
	// DefaultRedisRegisterPluginStopTimeout. If the timeout expires before the
	// lifecycle lock is acquired, no cleanup attempt has started and the caller
	// must call Stop again. Once an attempt starts, it may finish in the background.
	StopTimeout time.Duration

	Options *store.Config
	kv      store.Store

	lifecycleLock sync.Mutex
	storeLock     sync.Mutex
	dying         chan struct{}
	done          chan struct{}
	started       bool
	stopped       bool
	stopComplete  bool
	stopAttempt   *redisStopAttempt
	stopRequested atomic.Bool
}

type redisStopAttempt struct {
	done chan struct{}
	err  error
}

// DefaultRedisRegisterPluginStopTimeout is the total wait budget for Stop,
// including contention on lifecycle operations and registry cleanup.
const DefaultRedisRegisterPluginStopTimeout = 5 * time.Second

var (
	errRedisBasePathEmpty         = errors.New("redis register plugin BasePath can't be empty")
	errRedisRegisterPluginStopped = errors.New("redis register plugin has stopped")
	// ErrRedisRegisterPluginStopTimeout identifies a bounded Stop wait. When the
	// timeout occurs during lifecycle-lock contention, callers must retry Stop to
	// start cleanup. If cleanup already started, it remains serialized in the
	// background and a later Stop observes it or retries a failed attempt.
	ErrRedisRegisterPluginStopTimeout = errors.New("redis register plugin stop timeout")
)

func (p *RedisRegisterPlugin) validateBasePath() error {
	if strings.TrimSpace(p.BasePath) == "" {
		return errRedisBasePathEmpty
	}
	return nil
}

// Start starts to connect redis cluster
func (p *RedisRegisterPlugin) Start() error {
	if p.stopRequested.Load() {
		return errRedisRegisterPluginStopped
	}

	p.lifecycleLock.Lock()
	defer p.lifecycleLock.Unlock()

	if p.stopRequested.Load() || p.stopped {
		return errRedisRegisterPluginStopped
	}
	if p.started {
		return nil
	}
	if err := p.validateBasePath(); err != nil {
		return err
	}

	p.done = make(chan struct{})
	p.dying = make(chan struct{})

	if p.kv == nil {
		kv, err := libkv.NewStore(store.REDIS, p.RedisServers, p.Options)
		if err != nil {
			log.Errorf("cannot create redis registry: %v", err)
			close(p.done)
			return err
		}
		p.kv = kv
	}

	err := p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	if err != nil && !strings.Contains(err.Error(), "Not a file") {
		log.Errorf("cannot create redis path %s: %v", p.BasePath, err)
		p.kv.Close()
		p.kv = nil
		close(p.done)
		return err
	}

	p.started = true
	if p.UpdateInterval > 0 {
		kv := p.kv
		dying := p.dying
		done := p.done
		basePath := p.BasePath
		serviceAddress := p.ServiceAddress
		updateInterval := p.UpdateInterval
		go func() {
			defer close(done)
			ticker := time.NewTicker(updateInterval)

			defer ticker.Stop()

			// refresh service TTL
			for {
				select {
				case <-dying:
					return
				case <-ticker.C:
					extra := make(map[string]string)
					if p.Metrics != nil {
						extra["calls"] = fmt.Sprintf("%.2f", metrics.GetOrRegisterMeter("calls", p.Metrics).RateMean())
						extra["connections"] = fmt.Sprintf("%.2f", metrics.GetOrRegisterMeter("connections", p.Metrics).RateMean())
					}

					p.storeLock.Lock()
					p.stateLock.RLock()
					services := append([]string(nil), p.Services...)
					metas := make(map[string]string, len(services))
					for _, name := range services {
						metas[name] = p.metas[name]
					}
					p.stateLock.RUnlock()

					// set this same metrics for all services at this server
					for _, name := range services {
						nodePath := fmt.Sprintf("%s/%s/%s", basePath, name, serviceAddress)
						kvPair, err := kv.Get(nodePath)
						if err != nil {
							log.Infof("can't get data of node: %s, because of %v", nodePath, err.Error())

							err = kv.Put(nodePath, []byte(metas[name]), &store.WriteOptions{TTL: updateInterval * 2})
							if err != nil {
								log.Errorf("cannot re-create redis path %s: %v", nodePath, err)
							}
						} else {
							v, _ := url.ParseQuery(string(kvPair.Value))
							for key, value := range extra {
								v.Set(key, value)
							}
							if err := kv.Put(nodePath, []byte(v.Encode()), &store.WriteOptions{TTL: updateInterval * 2}); err != nil {
								log.Errorf("cannot refresh redis path %s: %v", nodePath, err)
							}
						}
					}
					p.storeLock.Unlock()
				}
			}
		}()
	} else {
		close(p.done)
	}

	return nil
}

// Stop unregister all services.
func (p *RedisRegisterPlugin) Stop() error {
	p.stopRequested.Store(true)
	timeout := p.StopTimeout
	if timeout <= 0 {
		timeout = DefaultRedisRegisterPluginStopTimeout
	}
	deadline := time.Now().Add(timeout)

	if !p.lockLifecycleUntil(deadline) {
		return fmt.Errorf("%w after %s", ErrRedisRegisterPluginStopTimeout, timeout)
	}
	if p.stopComplete {
		p.lifecycleLock.Unlock()
		return nil
	}

	if !p.stopped {
		p.stopped = true
		p.started = false
		if p.dying != nil {
			close(p.dying)
		}
	}

	attempt := p.stopAttempt
	if attempt == nil {
		attempt = &redisStopAttempt{done: make(chan struct{})}
		p.stopAttempt = attempt
		go p.runStopAttempt(attempt, p.done)
	}
	p.lifecycleLock.Unlock()

	select {
	case <-attempt.done:
		return attempt.err
	default:
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return fmt.Errorf("%w after %s", ErrRedisRegisterPluginStopTimeout, timeout)
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-attempt.done:
		return attempt.err
	case <-timer.C:
		// Prefer a just-completed result over a boundary timeout.
		select {
		case <-attempt.done:
			return attempt.err
		default:
		}
		return fmt.Errorf("%w after %s", ErrRedisRegisterPluginStopTimeout, timeout)
	}
}

func (p *RedisRegisterPlugin) lockLifecycleUntil(deadline time.Time) bool {
	for {
		if p.lifecycleLock.TryLock() {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		pause := time.Millisecond
		if remaining < pause {
			pause = remaining
		}
		timer := time.NewTimer(pause)
		<-timer.C
	}
}

func (p *RedisRegisterPlugin) runStopAttempt(attempt *redisStopAttempt, refreshDone <-chan struct{}) {
	if refreshDone != nil {
		<-refreshDone
	}
	attempt.err = p.cleanupRegistry()

	p.lifecycleLock.Lock()
	if attempt.err == nil {
		p.stopComplete = true
	}
	if p.stopAttempt == attempt {
		p.stopAttempt = nil
	}
	close(attempt.done)
	p.lifecycleLock.Unlock()
}

func (p *RedisRegisterPlugin) cleanupRegistry() error {
	p.storeLock.Lock()
	defer p.storeLock.Unlock()

	p.stateLock.RLock()
	services := append([]string(nil), p.Services...)
	p.stateLock.RUnlock()

	var errs []error
	if p.kv != nil {
		for _, name := range services {
			nodePath := fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
			exist, err := p.kv.Exists(nodePath)
			if err != nil {
				log.Errorf("cannot delete path %s: %v", nodePath, err)
				errs = append(errs, fmt.Errorf("check redis path %s: %w", nodePath, err))
				continue
			}
			if exist {
				if err := p.kv.Delete(nodePath); err != nil {
					log.Errorf("cannot delete path %s: %v", nodePath, err)
					errs = append(errs, fmt.Errorf("delete redis path %s: %w", nodePath, err))
					continue
				}
				log.Infof("delete path %s", nodePath)
			}
		}

		stopErr := errors.Join(errs...)
		if stopErr != nil {
			return stopErr
		}

		p.kv.Close()
		p.kv = nil
	}
	return nil
}

// HandleConnAccept handles connections from clients
func (p *RedisRegisterPlugin) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	if p.Metrics != nil {
		metrics.GetOrRegisterMeter("connections", p.Metrics).Mark(1)
	}
	return conn, true
}

// PreCall handles rpc call from clients
func (p *RedisRegisterPlugin) PreCall(_ context.Context, _, _ string, args interface{}) (interface{}, error) {
	if p.Metrics != nil {
		metrics.GetOrRegisterMeter("calls", p.Metrics).Mark(1)
	}
	return args, nil
}

// Register handles registering event.
// this service is registered at BASE/serviceName/thisIpAddress node
func (p *RedisRegisterPlugin) Register(name string, rcvr interface{}, metadata string) (err error) {
	if strings.TrimSpace(name) == "" {
		err = errors.New("Register service `name` can't be empty")
		return
	}
	if p.stopRequested.Load() {
		return errRedisRegisterPluginStopped
	}

	p.lifecycleLock.Lock()
	defer p.lifecycleLock.Unlock()
	if p.stopRequested.Load() || p.stopped {
		return errRedisRegisterPluginStopped
	}
	if err := p.validateBasePath(); err != nil {
		return err
	}

	if p.kv == nil {
		redis.Register()
		kv, err := libkv.NewStore(store.REDIS, p.RedisServers, p.Options)
		if err != nil {
			log.Errorf("cannot create redis registry: %v", err)
			return err
		}
		p.kv = kv
	}

	p.storeLock.Lock()
	defer p.storeLock.Unlock()

	err = p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	if err != nil && !strings.Contains(err.Error(), "Not a file") {
		log.Errorf("cannot create redis path %s: %v", p.BasePath, err)
		return err
	}

	nodePath := fmt.Sprintf("%s/%s", p.BasePath, name)
	err = p.kv.Put(nodePath, []byte(name), &store.WriteOptions{IsDir: true})
	if err != nil && !strings.Contains(err.Error(), "Not a file") {
		log.Errorf("cannot create redis path %s: %v", nodePath, err)
		return err
	}

	nodePath = fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
	err = p.kv.Put(nodePath, []byte(metadata), &store.WriteOptions{TTL: p.UpdateInterval * 2})
	if err != nil {
		log.Errorf("cannot create redis path %s: %v", nodePath, err)
		return err
	}

	p.stateLock.Lock()
	p.Services = append(p.Services, name)
	if p.metas == nil {
		p.metas = make(map[string]string)
	}
	p.metas[name] = metadata
	p.stateLock.Unlock()
	return
}

func (p *RedisRegisterPlugin) Unregister(name string) (err error) {
	if p.stopRequested.Load() {
		return nil
	}

	p.lifecycleLock.Lock()
	defer p.lifecycleLock.Unlock()

	if p.stopRequested.Load() || p.stopped {
		return nil
	}

	p.stateLock.RLock()
	serviceCount := len(p.Services)
	p.stateLock.RUnlock()
	if serviceCount == 0 {
		return nil
	}

	if strings.TrimSpace(name) == "" {
		err = errors.New("Register service `name` can't be empty")
		return
	}
	if p.kv == nil {
		redis.Register()
		kv, err := libkv.NewStore(store.REDIS, p.RedisServers, p.Options)
		if err != nil {
			log.Errorf("cannot create redis registry: %v", err)
			return err
		}
		p.kv = kv
	}

	p.storeLock.Lock()
	defer p.storeLock.Unlock()

	err = p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	if err != nil && !strings.Contains(err.Error(), "Not a file") {
		log.Errorf("cannot create redis path %s: %v", p.BasePath, err)
		return err
	}

	nodePath := fmt.Sprintf("%s/%s", p.BasePath, name)
	err = p.kv.Put(nodePath, []byte(name), &store.WriteOptions{IsDir: true})
	if err != nil && !strings.Contains(err.Error(), "Not a file") {
		log.Errorf("cannot create redis path %s: %v", nodePath, err)
		return err
	}

	nodePath = fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)

	err = p.kv.Delete(nodePath)
	if err != nil {
		log.Errorf("cannot remove redis path %s: %v", nodePath, err)
		return err
	}

	p.stateLock.Lock()
	services := make([]string, 0, len(p.Services)-1)
	for _, s := range p.Services {
		if s != name {
			services = append(services, s)
		}
	}
	p.Services = services
	if p.metas == nil {
		p.metas = make(map[string]string)
	}
	delete(p.metas, name)
	p.stateLock.Unlock()
	return
}
