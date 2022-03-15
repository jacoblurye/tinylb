package tinylb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var log = logrus.New()

func init() {
	var level logrus.Level
	if input, ok := os.LookupEnv("LOG_LEVEL"); ok {
		parsedLevel, err := logrus.ParseLevel(input)
		if err != nil {
			log.Panic(err)
		}
		level = parsedLevel
	} else {
		level = logrus.FatalLevel
	}
	log.SetLevel(level)
}

type testTCPListener struct {
	*net.TCPListener
}

func randomTestTCPListener() *testTCPListener {
	// ':0' resolves to a random available port.
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		log.Panic(err)
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Panic(err)
	}
	return &testTCPListener{TCPListener: listener}
}

func (l *testTCPListener) Addr() *net.TCPAddr {
	return l.TCPListener.Addr().(*net.TCPAddr)
}

func (l *testTCPListener) bothConns() (*net.TCPConn, *net.TCPConn) {
	rconn, err := net.DialTCP("tcp", nil, l.Addr())
	if err != nil {
		log.Panic(err)
	}
	lconn, err := l.AcceptTCP()
	if err != nil {
		log.Panic(err)
	}
	return rconn, lconn
}

func randomTargetGroupConfig(name string, timeout uint, numBackends int) TargetGroupConfig {
	targets := make([]TargetConfig, numBackends)
	for i := range targets {
		// Get a free port for this backend
		listener := randomTestTCPListener()
		defer listener.Close()
		targets[i] = TargetConfig{
			Host: listener.Addr().IP.String(),
			Port: uint(listener.Addr().Port),
		}
	}
	listener := randomTestTCPListener()
	defer listener.Close()
	port := listener.Addr().Port
	return TargetGroupConfig{Name: name, Port: uint(port), Timeout: timeout, Targets: targets}
}

func targetWithListener(listener *testTCPListener) *Target {
	return &Target{
		addr:    listener.Addr(),
		timeout: 5 * time.Second,
		log:     log.WithField("target", listener.Addr()),
	}
}

func backendHTTPServer(addr string) *http.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		message, err := io.ReadAll(r.Body)
		if err != nil {
			log.Panic(err)
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `%s %s`, message, addr)
	})
	log.Info("initializing backend server ", addr)
	srv := &http.Server{Addr: addr, Handler: handler}
	go srv.ListenAndServe()

	return srv
}

func waitForTargetHealth(targetGroup *TargetGroup) {
	for {
		targetGroup.RLock()
		nhealthy := len(targetGroup.targetRing.GetMembers())
		ntargets := len(targetGroup.targets)
		targetGroup.RUnlock()
		if nhealthy == ntargets {
			break
		}
		time.Sleep(1 * time.Second)
	}
}

// countTargetHits sends a bunch of connections to the target group - enough that,
// in theory, at least one connection should be routed to every target.
func countTargetHits(ctx context.Context, targetGroup *TargetGroup) int {
	// Prevent the HTTP client from reusing connections so that each request
	// might be assigned to a different target.
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DisableKeepAlives = true
	client := &http.Client{Transport: t}

	message := "hello, target"
	hitTargets := make(map[string]int, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 15*len(targetGroup.targets); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			targetGroup.RLock()
			addr := targetGroup.listener.Addr()
			targetGroup.RUnlock()
			url := fmt.Sprintf("http://%s/echo", addr)
			res, err := client.Post(url, "text/plain", strings.NewReader(message))
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					return
				}
				log.Panic(err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil {
				log.Panic(err)
			}
			mu.Lock()
			hitTargets[string(body)]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	return len(hitTargets)
}

func TestLoadBalancerOpen(t *testing.T) {
	config := Config{
		ControlPlane: ControlPlaneConfig{Port: 5555},
		TargetGroups: []TargetGroupConfig{
			randomTargetGroupConfig("tg-1", 1, 3),
			randomTargetGroupConfig("tg-2", 1, 2),
		},
	}
	lb, err := Open(config, log)
	if err != nil {
		log.Panic(err)
	}
	defer lb.Close()

	// Wait for control plane to start up.
	time.Sleep(3 * time.Second)

	// Check that both target groups are present and listening for connections.
	for _, targetGroupConfig := range config.TargetGroups {
		targetGroup, ok := lb.targetGroups[targetGroupConfig.Name]
		assert.True(t, ok)
		_, err := net.DialTCP("tcp", nil, targetGroup.listener.Addr().(*net.TCPAddr))
		if err != nil {
			log.Panic(err)
		}
	}

	// Check that the control plane server is accepting connections.
	configBytes, err := json.Marshal(config)
	if err != nil {
		log.Panic(err)
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/config", config.ControlPlane.Port)
	res, err := http.Post(url, "application/json", bytes.NewReader(configBytes))
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, res.StatusCode, http.StatusOK)
}

func TestLoadBalancerUpdateConfig(t *testing.T) {
	config := Config{
		ControlPlane: ControlPlaneConfig{Port: 5555},
		TargetGroups: []TargetGroupConfig{
			randomTargetGroupConfig("tg-1", 1, 3),
			randomTargetGroupConfig("tg-2", 1, 2),
		},
	}
	lb, err := Open(config, log)
	if err != nil {
		log.Panic(err)
	}
	defer lb.Close()

	// Wait for control plane to start up.
	time.Sleep(3 * time.Second)

	// Changing a target group port is not allowed.
	config.TargetGroups[0].Port++
	err = lb.UpdateConfig(config)
	assert.Equal(t, err.Error(), "cannot update target group port on-the-fly")
	config.TargetGroups[0].Port--

	// Changing the control plane port is not allowed.
	config.ControlPlane.Port++
	err = lb.UpdateConfig(config)
	assert.Equal(t, err.Error(), "cannot update control plane port on-the-fly")
	config.ControlPlane.Port--

	// Remove one target group, remove all but one target from the remaining target group,
	// and add a new target group to the load balancer config.
	config.TargetGroups = config.TargetGroups[:1]
	config.TargetGroups[0].Targets = config.TargetGroups[0].Targets[:1]
	config.TargetGroups = append(config.TargetGroups, randomTargetGroupConfig("tg-3", 1, 2))

	// Target groups are created, updated, and removed as expected.
	err = lb.UpdateConfig(config)
	if err != nil {
		log.Panic(err)
	}
	assert.Len(t, lb.targetGroups, 2)
	assert.Contains(t, lb.targetGroups, "tg-1")
	assert.Contains(t, lb.targetGroups, "tg-3")
	assert.NotContains(t, lb.targetGroups, "tg-2")
	assert.Len(t, lb.targetGroups["tg-1"].targets, 1)
	assert.Len(t, lb.targetGroups["tg-3"].targets, 2)
}

func TestLoadBalancerClose(t *testing.T) {
	config := Config{
		ControlPlane: ControlPlaneConfig{Port: 5555},
		TargetGroups: []TargetGroupConfig{
			randomTargetGroupConfig("tg-1", 30, 3),
			randomTargetGroupConfig("tg-2", 15, 2),
		},
	}
	lb, err := Open(config, log)
	if err != nil {
		log.Panic(err)
	}
	defer lb.Close()

	// Wait for control plane to start up.
	time.Sleep(3 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, targetGroup := range lb.targetGroups {
		// Start backend servers for targets in this target group.
		for addr := range targetGroup.targets {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				srv := backendHTTPServer(addr)
				<-ctx.Done()
				srv.Close()
			}(addr)
		}
		waitForTargetHealth(targetGroup)
	}

	// Open a connection to each target group and create a goroutine to close it in
	// the background.
	for _, target := range lb.targetGroups {
		conn, err := net.DialTCP("tcp", nil, target.listener.Addr().(*net.TCPAddr))
		if err != nil {
			log.Panic(err)
		}
		wg.Add(1)
		go func(conn *net.TCPConn) {
			defer wg.Done()
			<-ctx.Done()
			// Wait for 1 second before closing the connection.
			time.Sleep(1 * time.Second)
			conn.Close()
		}(conn)
	}
	// Client target group connections will close after 1 second.
	cancel()

	// Closing the lb doesn't interfere with existing target group connections.
	lb.Close()

	wg.Wait()
}

func TestLoadBalancerControlPlaneErrors(t *testing.T) {
	config := Config{
		ControlPlane: ControlPlaneConfig{Port: 5555},
		TargetGroups: []TargetGroupConfig{
			randomTargetGroupConfig("tg-1", 30, 3),
		},
	}
	_, err := Open(config, log)
	if err != nil {
		log.Panic(err)
	}

	// Wait for control plane to start up.
	time.Sleep(3 * time.Second)

	url := fmt.Sprintf("http://127.0.0.1:%d/config", config.ControlPlane.Port)

	// Errors on non-POST requests.
	res, err := http.Get(url)
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, res.StatusCode, http.StatusMethodNotAllowed)

	// Errors on malformed JSON.
	res, err = http.Post(url, "application/json", strings.NewReader("{]"))
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, res.StatusCode, http.StatusUnprocessableEntity)

	// Errors on target port updates.
	config.TargetGroups[0].Port++
	configBytes, err := json.Marshal(config)
	if err != nil {
		log.Panic(err)
	}
	res, err = http.Post(url, "application/json", bytes.NewReader(configBytes))
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, res.StatusCode, http.StatusBadRequest)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, string(body), `{"error":"cannot update target group port on-the-fly"}`)
	config.TargetGroups[0].Port--

	// Errors on control plane port updates.
	config.ControlPlane.Port++
	configBytes, err = json.Marshal(config)
	if err != nil {
		log.Panic(err)
	}
	res, err = http.Post(url, "application/json", bytes.NewReader(configBytes))
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, res.StatusCode, http.StatusBadRequest)
	defer res.Body.Close()
	body, err = io.ReadAll(res.Body)
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, string(body), `{"error":"cannot update control plane port on-the-fly"}`)
}

func TestTargetGroupOpenTargetGroup(t *testing.T) {
	config := randomTargetGroupConfig("tg", 30, 3)

	// A targetGroup group is opened with the expected initial state.
	targetGroup, err := openTargetGroup(config, log)
	defer targetGroup.drain()
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, targetGroup.listener.Addr().String(), fmt.Sprintf("127.0.0.1:%d", config.Port))
	assert.Len(t, targetGroup.targets, 3)
	assert.Len(t, targetGroup.targetRing.GetMembers(), 0)

	// A second target group can't be opened with the same config.
	_, err = openTargetGroup(config, log)
	assert.NotEmpty(t, err)

	// Targets get added to the targetRing as they become healthy.
	healthyCount := 0
	for _, target := range targetGroup.targets {
		// Open a listener for this target's backend address.
		listener, err := net.ListenTCP("tcp", target.addr)
		if err != nil {
			log.Panic(err)
		}
		defer listener.Close()
		listener.AcceptTCP()

		// Wait for a health check.
		time.Sleep(2 * time.Second)

		// backendRing increased in size by 1.
		healthyCount++
		assert.Len(t, targetGroup.targetRing.GetMembers(), healthyCount)
	}
}

func TestTargetGroupListener(t *testing.T) {
	config := randomTargetGroupConfig("tg", 5, 3)
	targetGroup, err := openTargetGroup(config, log)
	defer targetGroup.drain()
	if err != nil {
		log.Panic(err)
	}

	// New connections are closed if no targets are healthy.
	rconn, _ := net.DialTCP("tcp", nil, targetGroup.listener.Addr().(*net.TCPAddr))
	time.Sleep(1 * time.Second)
	one := make([]byte, 1)
	_, err = rconn.Read(one)
	assert.Equal(t, err, io.EOF)

	// Open an HTTP server for every target.
	for addr := range targetGroup.targets {
		server := backendHTTPServer(addr)
		defer server.Close()
	}
	waitForTargetHealth(targetGroup)

	assert.Equal(t, countTargetHits(context.Background(), targetGroup), len(targetGroup.targetRing.GetMembers()))

	// Make targets unhealthy one at a time until there's only one left, checking
	// that new connections still succeed.
	numTargets := len(targetGroup.targets)
	for numRemoved := 1; numRemoved < numTargets; numRemoved++ {
		config.Targets = config.Targets[1:]
		assert.Empty(t, targetGroup.updateConfig(config))
		assert.Len(t, targetGroup.targets, numTargets-numRemoved)
		waitForTargetHealth(targetGroup)
		assert.Equal(t, countTargetHits(context.Background(), targetGroup), len(targetGroup.targetRing.GetMembers()))
	}
}

func TestTargetGroupUpdateConfig(t *testing.T) {
	startConfig := randomTargetGroupConfig("tg", 1, 3)
	endConfig := randomTargetGroupConfig("tg", 1, 1)

	log.Infof("start %+v", startConfig)
	log.Infof("end %+v", endConfig)

	targetGroup, err := openTargetGroup(startConfig, log)
	if err != nil {
		log.Panic(err)
	}
	defer targetGroup.drain()

	// Ensure target group has expected starting state.
	assert.Equal(t, len(targetGroup.targets), len(startConfig.Targets))
	assert.Equal(t, targetGroup.Timeout, startConfig.Timeout)
	assert.Equal(t, targetGroup.listener.Addr().String(), fmt.Sprintf("127.0.0.1:%d", startConfig.Port))

	ctx, cancel := context.WithCancel(context.Background())

	// Start all target's backend servers.
	var wg sync.WaitGroup
	for addr := range targetGroup.targets {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			srv := backendHTTPServer(addr)
			<-ctx.Done()
			srv.Close()
		}(addr)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		addr := fmt.Sprintf("%s:%d", endConfig.Targets[0].Host, endConfig.Targets[0].Port)
		srv := backendHTTPServer(addr)
		<-ctx.Done()
		srv.Close()
	}()

	waitForTargetHealth(targetGroup)

	// Simulate clients continuously connecting to the target group to ensure no
	// connections get dropped while the config update is happening.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				countTargetHits(ctx, targetGroup)
			}
		}
	}()

	// Wait for some requests to be sent.
	time.Sleep(1 * time.Second)

	// endConfig will retain one of the original targets.
	endConfig.Targets = append(endConfig.Targets, startConfig.Targets[0])

	// endConfig has a different port from startConfig - this should be blocked.
	err = targetGroup.updateConfig(endConfig)
	assert.Equal(t, err.Error(), "cannot update target group port on-the-fly")

	endConfig.Port = startConfig.Port
	err = targetGroup.updateConfig(endConfig)
	if err != nil {
		log.Panic(err)
	}

	waitForTargetHealth(targetGroup)

	// Ensure target group applied the config update correctly.
	assert.Equal(t, len(targetGroup.targets), len(endConfig.Targets))
	assert.Equal(t, targetGroup.Timeout, endConfig.Timeout)
	assert.Equal(t, targetGroup.listener.Addr().String(), fmt.Sprintf("127.0.0.1:%d", endConfig.Port))

	// Wait for some requests to be sent.
	time.Sleep(1 * time.Second)

	cancel()
	wg.Wait()
}

func TestTargetGroupDrain(t *testing.T) {
	config := randomTargetGroupConfig("tg", 30, 3)
	targetGroup, err := openTargetGroup(config, log)
	if err != nil {
		log.Panic(err)
	}

	// Open a listener for every target backend.
	for _, target := range targetGroup.targets {
		listener, err := net.ListenTCP("tcp", target.addr)
		if err != nil {
			log.Panic(err)
		}
		defer listener.Close()
	}

	waitForTargetHealth(targetGroup)

	// Open a connection to the target.
	rconn, err := net.DialTCP("tcp", nil, targetGroup.listener.Addr().(*net.TCPAddr))
	if err != nil {
		log.Panic(err)
	}
	time.Sleep(2 * time.Second)
	order := make(chan int, 2)
	go func() {
		time.Sleep(2 * time.Second)
		order <- 1
		rconn.Close()
	}()
	targetGroup.drain()
	order <- 2

	// Check that the connection closed before the target group finished draining.
	assert.Equal(t, <-order, 1)
	assert.Equal(t, <-order, 2)

	// Draining a target group twice doesn't log.Panicf."%s",
	targetGroup.drain()
}

func TestTargetGroupSessionStickiness(t *testing.T) {
	config := randomTargetGroupConfig("tg", 30, 3)
	targetGroup, err := openTargetGroup(config, log)
	if err != nil {
		log.Panic(err)
	}

	// Open an HTTP server for every target backend.
	for addr := range targetGroup.targets {
		server := backendHTTPServer(addr)
		defer server.Close()
	}
	waitForTargetHealth(targetGroup)

	clientAddr, _ := net.ResolveTCPAddr("tcp", "localhost:0")
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				LocalAddr: clientAddr,
				Timeout:   1 * time.Second,
				KeepAlive: 0 * time.Second,
			}).DialContext,
		},
	}
	request := func() string {
		url := fmt.Sprintf("http://%s/echo", targetGroup.listener.Addr().String())
		res, err := client.Post(url, "text/plain", strings.NewReader("hello"))
		if err != nil {
			log.Panic(err)
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			log.Panic(err)
		}
		return string(body)
	}

	// Multiple connections from the same client are routed to the same backend.
	// Note: the HTTP backend servers send their address in their responses, so
	// identical responses implies requests were routed to the same target.
	response := request()
	assert.Equal(t, response, request())

	// Add another target to the target group and open a backend for it.
	targetAddr := randomTestTCPListener().Addr()
	if err != nil {
		log.Panic(err)
	}
	config.Targets = append(config.Targets, TargetConfig{
		Host: targetAddr.IP.String(),
		Port: uint(targetAddr.Port),
	})
	err = targetGroup.updateConfig(config)
	if err != nil {
		log.Panic(err)
	}
	server := backendHTTPServer(targetAddr.String())
	defer server.Close()
	waitForTargetHealth(targetGroup)

	// Client routing is stable when another target is added to the target group.
	assert.Equal(t, response, request())
}

func TestTargetNewTarget(t *testing.T) {
	// Errors on an invalid host.
	_, err := newTarget(TargetConfig{Host: "foobar", Port: 8080}, 60, logrus.NewEntry(log))
	assert.NotEmpty(t, err)

	// Produces a target with the expected configurations.
	target, err := newTarget(TargetConfig{Host: "127.0.0.1", Port: 8080}, 60, logrus.NewEntry(log))
	if err != nil {
		log.Panic(err)
	}
	assert.Equal(t, target.addr.String(), "127.0.0.1:8080")
	assert.Equal(t, target.timeout, 60*time.Second)
}

func TestTargetHealth(t *testing.T) {
	listener := randomTestTCPListener()
	defer listener.Close()
	target := targetWithListener(listener)

	// Health check should succeed if target is accepting connections.
	assert.True(t, target.healthy())

	listener.Close()

	// Health check should fail if target is not accepting connections.
	assert.False(t, target.healthy())
}

func TestTargetDrain(t *testing.T) {
	targetListener := randomTestTCPListener()
	defer targetListener.Close()
	target := targetWithListener(targetListener)

	// Drain does not block if no connections are open on this target.
	target.drain()

	// Proxy two connections to the target and leave them open.
	targetGroupListener := randomTestTCPListener()
	defer targetGroupListener.Close()
	targetGroupRemoteConn1, targetGroupLocalConn1 := targetGroupListener.bothConns()
	defer targetGroupRemoteConn1.Close()
	targetGroupRemoteConn2, targetGroupLocalConn2 := targetGroupListener.bothConns()
	defer targetGroupRemoteConn2.Close()
	go target.proxy(targetGroupLocalConn1)
	go target.proxy(targetGroupLocalConn2)
	time.Sleep(500 * time.Millisecond)

	// Drain blocks until all open proxy connections to this target close.
	order := make(chan int, 3)
	go func() {
		order <- 1
		targetGroupLocalConn1.Close()
		order <- 2
		targetGroupLocalConn2.Close()
	}()
	target.drain()
	order <- 3

	assert.Equal(t, <-order, 1)
	assert.Equal(t, <-order, 2)
	assert.Equal(t, <-order, 3)
}

func TestTargetProxy(t *testing.T) {
	targetListener := randomTestTCPListener()
	defer targetListener.Close()
	target := targetWithListener(targetListener)

	targetGroupListener := randomTestTCPListener()
	defer targetGroupListener.Close()
	targetGroupRemoteConn, targetGroupLocalConn := targetGroupListener.bothConns()
	defer targetGroupRemoteConn.Close()

	// Send data back and forth between a target group connection and a target backend connection.
	send := make(chan []byte)
	recv := make(chan []byte)
	wrote := make(chan bool)
	read := make(chan bool)
	sendMessage := []byte("hello, backend")
	recvMessage := []byte("hello, frontend")
	go func() {
		targetGroupRemoteConn.Write(sendMessage)
		wrote <- true
		buff := make([]byte, len(recvMessage))
		_, err := targetGroupRemoteConn.Read(buff)
		if err != nil {
			log.Panic(err)
		}
		read <- true
		recv <- buff
	}()
	go func() {
		targetConn, err := targetListener.AcceptTCP()
		if err != nil {
			log.Panic(err)
		}
		defer targetConn.Close()
		<-wrote
		buff := make([]byte, len(sendMessage))
		_, err = targetConn.Read(buff)
		if err != nil {
			log.Panic(err)
		}
		targetConn.Write(recvMessage)
		<-read
		send <- buff
	}()
	target.proxy(targetGroupLocalConn)

	// The backend remote connection receives data sent to a proxied target group connection.
	assert.Equal(t, <-send, sendMessage)
	// The target group remote connection receives data sent from the backend.
	assert.Equal(t, <-recv, recvMessage)

	// Target proxy calls time out after period of inactivity.
	target.timeout = 1 * time.Second
	_, targetGroupLocalConn = targetGroupListener.bothConns()
	timedout := make(chan bool)
	go func() {
		target.proxy(targetGroupLocalConn)
		timedout <- true
	}()
	select {
	case <-time.After(target.timeout + 1*time.Second):
		assert.Fail(t, "proxy failed to time out")
	case <-timedout:
	}
}
