package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/internal/testhook"
	"github.com/t4db/t4/internal/testutil"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func readyzServer(t *testing.T, node *t4.Node) string {
	t.Helper()
	addr := testutil.FreeAddr(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); serveMetrics(ctx, addr, node, false) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Error("metrics server did not stop")
		}
	})
	url := "http://" + addr
	deadline := time.Now().Add(5 * time.Second)
	for {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url+"/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := (&http.Client{Timeout: 4 * time.Second}).Do(req)
		if err == nil {
			_ = resp.Body.Close()
			return url
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics startup: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func readyzStatus(t *testing.T, url string, want int) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Timeout: 4 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != want {
		t.Fatalf("GET %s = %d, want %d: %s", url, resp.StatusCode, want, body)
	}
	if want == http.StatusServiceUnavailable && strings.HasSuffix(url, "/readyz") && string(body) != "not ready\n" {
		t.Fatalf("readiness exposed unexpected error: %q", body)
	}
}

func TestReadyzStandaloneLifecycle(t *testing.T) {
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	url := readyzServer(t, node)
	rev := node.CurrentRevision()
	readyzStatus(t, url+"/readyz", http.StatusOK)
	readyzStatus(t, url+"/healthz", http.StatusOK)
	leaderStatus := http.StatusServiceUnavailable
	if node.IsLeader() {
		leaderStatus = http.StatusOK
	}
	readyzStatus(t, url+"/healthz/leader", leaderStatus)
	readyzStatus(t, url+"/metrics", http.StatusOK)
	if got := node.CurrentRevision(); got != rev {
		t.Fatalf("readiness changed revision: %d -> %d", rev, got)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	readyzStatus(t, url+"/readyz", http.StatusServiceUnavailable)
	readyzStatus(t, url+"/healthz", http.StatusServiceUnavailable)
}

type readyzCluster struct {
	nodes []*t4.Node
	urls  []string
}

func newReadyzCluster(t *testing.T) readyzCluster {
	t.Helper()
	store := object.NewMem()
	var c readyzCluster
	for i := 0; i < 3; i++ {
		addr := testutil.FreeAddr(t)
		node, err := t4.Open(t4.Config{
			DataDir: t.TempDir(), ObjectStore: store, NodeID: fmt.Sprintf("readyz-%d", i),
			PeerListenAddr: addr, AdvertisePeerAddr: addr, FollowerMaxRetries: 2,
			PeerBufferSize: 1000,
			// Readiness must still prove the stronger path in serializable mode.
			ReadConsistency: t4.ReadConsistencySerializable,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = node.Close() })
		c.nodes = append(c.nodes, node)
		c.urls = append(c.urls, readyzServer(t, node))
	}
	return c
}

func (c readyzCluster) leader(t *testing.T, exclude int) int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		leader, count := -1, 0
		for i, node := range c.nodes {
			if i != exclude && node.IsLeader() {
				leader = i
				count++
			}
		}
		if count == 1 {
			t.Logf("elected leader: node %d", leader)
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected exactly one surviving leader within 30s")
	return -1
}

func (c readyzCluster) catchUp(t *testing.T, rev int64, exclude int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for i, node := range c.nodes {
		if i == exclude {
			continue
		}
		if err := node.WaitForRevision(ctx, rev); err != nil {
			t.Fatalf("node %d catch-up to %d: %v", i, rev, err)
		}
		readyzStatus(t, c.urls[i]+"/readyz", http.StatusOK)
	}
}

func readyzGet(t *testing.T, node *t4.Node, key, value string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(t4etcd.NewServerOptions(nil, nil)...)
	t4etcd.New(node, nil, nil).Register(srv)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{lis.Addr().String()}, DialTimeout: 4 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, key) // Default etcd GET is linearizable, even in serializable node mode.
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != value {
		t.Fatalf("linearizable GET %q: %v, want %q", key, resp.Kvs, value)
	}
}

func TestReadyzFollowerApplyLag(t *testing.T) {
	// This test owns the global follower hook; it must not run in parallel.
	c := newReadyzCluster(t)
	leader := c.leader(t, -1)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	rev, err := c.nodes[leader].Put(ctx, "/readyz/baseline", []byte("baseline"), 0)
	if err != nil {
		t.Fatal(err)
	}
	c.catchUp(t, rev, -1)

	release, entered := make(chan struct{}), make(chan struct{})
	var claimed atomic.Bool
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	hook := func(entries []wal.Entry) {
		for _, e := range entries {
			// Claim without holding a mutex while blocked: the other follower must ACK/apply.
			if e.Op == wal.OpCreate && e.Key == "/readyz/held" && claimed.CompareAndSwap(false, true) {
				close(entered)
				<-release
				return
			}
		}
	}
	testhook.BeforeFollowerApply.Store(&hook)
	t.Cleanup(func() { unblock(); testhook.BeforeFollowerApply.Store(nil) })
	rev, err = c.nodes[leader].Put(ctx, "/readyz/held", []byte("receipt"), 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("follower did not enter apply hook")
	}
	blocked := -1
	for i, node := range c.nodes {
		if i != leader && node.CurrentRevision() < rev {
			// The unblocked follower can still be finishing its apply. Give it time.
			waitCtx, stop := context.WithTimeout(ctx, 300*time.Millisecond)
			err := node.WaitForRevision(waitCtx, rev)
			stop()
			if err != nil {
				if blocked != -1 {
					t.Fatal("more than one follower stalled")
				}
				blocked = i
			}
		}
	}
	if blocked == -1 {
		t.Fatal("no blocked follower found")
	}
	if got := c.nodes[blocked].CurrentRevision(); got < 0 || got >= rev {
		t.Fatalf("blocked revision=%d, leader revision=%d", got, rev)
	}
	if !c.nodes[leader].IsLeader() || c.nodes[leader].CurrentRevision() < rev {
		t.Fatal("leadership changed or leader did not advance")
	}

	start := time.Now()
	readyzStatus(t, c.urls[blocked]+"/readyz", http.StatusServiceUnavailable)
	elapsed := time.Since(start)
	if elapsed < 1800*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("stalled barrier took %v, want server-side 2s timeout", elapsed)
	}
	readyzStatus(t, c.urls[blocked]+"/healthz", http.StatusOK)
	readyzStatus(t, c.urls[blocked]+"/healthz/leader", http.StatusServiceUnavailable)
	readyzStatus(t, c.urls[blocked]+"/metrics", http.StatusOK)
	c.catchUp(t, rev, blocked)

	// Cancel after the request is written, while the same apply barrier is held.
	cancelCtx, stop := context.WithCancel(t.Context())
	written := make(chan struct{}, 1)
	trace := &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { written <- struct{}{} }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(cancelCtx, trace), http.MethodGet, c.urls[blocked]+"/readyz", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	go func() {
		resp, err := (&http.Client{Timeout: 4 * time.Second, Transport: transport}).Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		result <- err
	}()
	select {
	case <-written:
	case <-time.After(4 * time.Second):
		stop()
		t.Fatal("request was not written")
	}
	stop()
	select {
	case err := <-result:
		if err == nil {
			t.Error("cancelled stalled request succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("request cancellation did not terminate promptly")
	}

	unblock()
	c.catchUp(t, rev, -1)
	readyzGet(t, c.nodes[blocked], "/readyz/held", "receipt")
	if !c.nodes[leader].IsLeader() {
		t.Fatal("leadership unexpectedly changed during held follower test")
	}
}

func TestReadyzLeaderReplacement(t *testing.T) {
	c := newReadyzCluster(t)
	leader := c.leader(t, -1)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	rev, err := c.nodes[leader].Put(ctx, "/readyz/failover", []byte("committed-receipt"), 0)
	if err != nil {
		t.Fatal(err)
	}
	c.catchUp(t, rev, -1)
	if err := c.nodes[leader].Close(); err != nil {
		t.Fatal(err)
	}
	readyzStatus(t, c.urls[leader]+"/readyz", http.StatusServiceUnavailable)
	replacement := c.leader(t, leader)
	t.Logf("leader replacement: %d -> %d", leader, replacement)
	// Election precedes installing the new read index; require eventual readiness.
	deadline := time.Now().Add(30 * time.Second)
	for i, node := range c.nodes {
		if i == leader {
			continue
		}
		for {
			probeCtx, stop := context.WithTimeout(t.Context(), 2*time.Second)
			_, err := node.LinearizableRevision(probeCtx)
			stop()
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("survivor %d did not recover read index: %v", i, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		readyzStatus(t, c.urls[i]+"/readyz", http.StatusOK)
		readyzGet(t, node, "/readyz/failover", "committed-receipt")
	}
}
