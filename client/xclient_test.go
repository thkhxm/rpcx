package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"fmt"

	testutils "github.com/thkhxm/rpcx/v2/_testutils"
	"github.com/thkhxm/rpcx/v2/protocol"
	"github.com/thkhxm/rpcx/v2/server"
	"github.com/thkhxm/rpcx/v2/share"
)

type selectorUpdateDiscovery struct {
	initial []*KVPair
	updates chan []*KVPair
}

func (d *selectorUpdateDiscovery) GetServices() []*KVPair { return d.initial }
func (d *selectorUpdateDiscovery) WatchService() chan []*KVPair {
	return d.updates
}
func (d *selectorUpdateDiscovery) RemoveWatcher(chan []*KVPair) {}
func (d *selectorUpdateDiscovery) Clone(string) (ServiceDiscovery, error) {
	return d, nil
}
func (d *selectorUpdateDiscovery) SetFilter(ServiceDiscoveryFilter) {}
func (d *selectorUpdateDiscovery) Close()                           {}

func TestXClientSelectorUpdatesAreSynchronized(t *testing.T) {
	discovery := &selectorUpdateDiscovery{
		initial: []*KVPair{{Key: "tcp@127.0.0.1:10001"}},
		updates: make(chan []*KVPair),
	}
	xclient := NewXClient("Arith", Failfast, SelectByUser, discovery, DefaultOption)

	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			discovery.updates <- []*KVPair{{Key: fmt.Sprintf("tcp@127.0.0.1:%d", 10001+i)}}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if i%2 == 0 {
				xclient.SetSelector(newRandomSelector(nil))
				continue
			}
			xclient.ConfigGeoSelector(0, 0)
		}
	}()
	wg.Wait()

	if err := xclient.Close(); err != nil {
		t.Fatalf("close xclient: %v", err)
	}
}

func TestXClient_Thrift(t *testing.T) {
	s := server.NewServer()
	s.RegisterName("Arith", new(Arith), "")
	go s.Serve("tcp", "127.0.0.1:0")
	defer s.Close()
	time.Sleep(500 * time.Millisecond)

	addr := s.Address().String()

	opt := Option{
		Retries:        1,
		RPCPath:        share.DefaultRPCPath,
		ConnectTimeout: 10 * time.Second,
		SerializeType:  protocol.Thrift,
		CompressType:   protocol.None,
		BackupLatency:  10 * time.Millisecond,
	}

	d, err := NewPeer2PeerDiscovery("tcp@"+addr, "desc=a test service")
	if err != nil {
		t.Fatalf("failed to NewPeer2PeerDiscovery: %v", err)
	}

	xclient := NewXClient("Arith", Failtry, RandomSelect, d, opt)

	defer xclient.Close()

	args := testutils.ThriftArgs_{}
	args.A = 200
	args.B = 100

	reply := testutils.ThriftReply{}

	err = xclient.Call(context.Background(), "ThriftMul", &args, &reply)
	if err != nil {
		t.Fatalf("failed to call: %v", err)
	}

	fmt.Println(reply.C)
	if reply.C != 20000 {
		t.Fatalf("expect 20000 but got %d", reply.C)
	}
}

func TestXClient_IT(t *testing.T) {
	s := server.NewServer()
	s.RegisterName("Arith", new(Arith), "")
	go s.Serve("tcp", "127.0.0.1:0")
	defer s.Close()
	time.Sleep(500 * time.Millisecond)

	addr := s.Address().String()

	d, err := NewPeer2PeerDiscovery("tcp@"+addr, "desc=a test service")
	if err != nil {
		t.Fatalf("failed to NewPeer2PeerDiscovery: %v", err)
	}

	xclient := NewXClient("Arith", Failtry, RandomSelect, d, DefaultOption)

	defer xclient.Close()

	args := &Args{
		A: 10,
		B: 20,
	}

	reply := &Reply{}
	err = xclient.Call(context.Background(), "Mul", args, reply)
	if err != nil {
		t.Fatalf("failed to call: %v", err)
	}

	if reply.C != 200 {
		t.Fatalf("expect 200 but got %d", reply.C)
	}
}

func TestXClient_filterByStateAndGroup(t *testing.T) {
	servers := map[string]string{"a": "", "b": "state=inactive&ops=10", "c": "ops=20", "d": "group=test1&group=test&ops=20"}
	filterByStateAndGroup("test", servers)
	if _, ok := servers["b"]; ok {
		t.Error("has not remove inactive node")
	}
	if _, ok := servers["a"]; ok {
		t.Error("has not remove inactive node")
	}
	if _, ok := servers["c"]; ok {
		t.Error("has not remove inactive node")
	}
	if _, ok := servers["d"]; !ok {
		t.Error("node must be removed")
	}

	filterByStateAndGroup("test1", servers)

	if _, ok := servers["d"]; !ok {
		t.Error("node must be removed")
	}
}

func TestUncoverError(t *testing.T) {
	var e error = strErr("error")
	if uncoverError(e) {
		t.Fatalf("expect false but get true")
	}

	if uncoverError(context.DeadlineExceeded) {
		t.Fatalf("expect false but get true")
	}

	if uncoverError(context.Canceled) {
		t.Fatalf("expect false but get true")
	}

	e = errors.New("error")
	if !uncoverError(e) {
		t.Fatalf("expect true but get false")
	}
}
