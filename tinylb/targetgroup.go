package tinylb

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/sirupsen/logrus"
)

// TargetGroup listens for incoming connections on a specific port, assigning each
// new connection to a healthy target from the configured group of targets. TargetGroups
// use consistent hashing based on the incoming connection's source address to assign
// connections to targets, so the same client will always be routed to the same target
// as long as that target is healthy and present in the target group.
type TargetGroup struct {
	sync.RWMutex
	TargetGroupConfig

	// listener receives incoming TCP connections for this target.
	listener *net.TCPListener
	// targets is a map of all configured targets keyed by each target's address.
	targets map[string]*Target
	// targetRing is a hash ring containing only healthy targets.
	targetRing *consistent.Consistent
	// stop signals that all long-running goroutines for this target group should stop.
	stop bool
	// closed signals that the target group is no longer listening for new connections.
	closed chan bool

	log *logrus.Entry
}

// getTCPListener creates a net.TCPListener listening on `port`.
func getTCPListener(port uint) (*net.TCPListener, error) {
	laddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		return nil, err
	}
	return listener, nil
}

// hasher is used by the `consistent` package to assign ring members to partitions
// and keys to members.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// openTargetGroup creates a new target group with the provided configuration.
func openTargetGroup(config TargetGroupConfig, log *logrus.Logger) (*TargetGroup, error) {
	listener, err := getTCPListener(config.Port)
	if err != nil {
		return nil, err
	}

	targetRing := consistent.New(nil, consistent.Config{
		PartitionCount:    71,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	})

	target := &TargetGroup{
		TargetGroupConfig: config,
		listener:          listener,
		targets:           map[string]*Target{},
		targetRing:        targetRing,
		stop:              false,
		closed:            make(chan bool),
		log: log.WithFields(logrus.Fields{
			"target_group":      config.Name,
			"target_group_port": config.Port,
		}),
	}
	if err := target.updateConfig(config); err != nil {
		return nil, err
	}

	go target.listen()
	go target.healthChecks()

	return target, nil
}

// drain gracefully shuts down this target group and its targets, waiting
// for all existing connections to close.
func (tg *TargetGroup) drain() {
	defer tg.listener.Close()

	tg.log.Info("draining")

	if tg.shouldStop() {
		tg.log.Warn("already drained - noop")
		return
	}

	tg.Lock()
	tg.stop = true
	tg.Unlock()

	// Wait for listener to stop accepting connections
	<-tg.closed

	// Drain all targets
	var wg sync.WaitGroup
	for _, target := range tg.targets {
		wg.Add(1)
		func(t *Target) {
			t.drain()
			wg.Done()
		}(target)
	}
	wg.Wait()

	tg.log.Info("done draining")
}

// updateConfig applies the given config to this target group without downtime for
// existing or new connections.
func (tg *TargetGroup) updateConfig(config TargetGroupConfig) error {
	tg.Lock()
	defer tg.Unlock()

	tg.log.Info("updating config")

	// Don't allow on-the-fly target group port updates.
	if tg.Port != config.Port {
		return errors.New("cannot update target group port on-the-fly")
	}

	// Create new targets and keep track of them in a map for diffing with
	// the existing targets below.
	newTargets := make(map[string]*Target, len(config.Targets))
	for _, targetConfig := range config.Targets {
		target, err := newTarget(targetConfig, tg.Timeout, tg.log)
		if err != nil {
			return err
		}
		tg.targets[target.addr.String()] = target
		newTargets[target.addr.String()] = target
	}

	// Drain targets no longer present in the config.
	var drainWg sync.WaitGroup
	remove := make([]string, len(tg.targets))
	for addr, target := range tg.targets {
		if _, ok := newTargets[addr]; !ok {
			drainWg.Add(1)
			go func(t *Target) {
				t.drain()
				drainWg.Done()
			}(target)
			remove = append(remove, addr)
		}
	}

	// Overwrite the target group's config.
	tg.TargetGroupConfig = config

	// Remove drained targets.
	drainWg.Wait()
	for _, addr := range remove {
		delete(tg.targets, addr)
		tg.targetRing.Remove(addr)
	}

	tg.log.Info("done updating config")

	return nil
}

func (tg *TargetGroup) shouldStop() bool {
	tg.RLock()
	defer tg.RUnlock()
	return tg.stop
}

// listen listens for new TCP connections for this target group and proxies
// them to a healthy target.
func (tg *TargetGroup) listen() {
	defer tg.listener.Close()

	for !tg.shouldStop() {
		conn, err := tg.pollConnection()
		if err != nil {
			// Only log non-timeout errors, since timeouts are expected.
			if err, ok := err.(net.Error); !(ok && err.Timeout()) {
				tg.log.Error("error accepting connection: ", err)
			}
			continue
		}

		target, err := tg.getTarget(conn)
		if err != nil {
			tg.log.Error("error getting target: ", err)
			conn.Close()
			continue
		}

		go target.proxy(conn)
	}

	// Signal that no more connections will be accepted
	tg.closed <- true
}

// pollConnection waits for a new TCP connection to this target group's listener,
// timing out after 1 second if no new connection arrives.
func (tg *TargetGroup) pollConnection() (*net.TCPConn, error) {
	tg.RLock()
	defer tg.RUnlock()

	tg.listener.SetDeadline(time.Now().Add(1 * time.Second))
	return tg.listener.AcceptTCP()
}

// getTarget selects a healthy target for the given connection from this
// target group's hash ring. If no targets are healthy, it errors.
func (tg *TargetGroup) getTarget(conn *net.TCPConn) (*Target, error) {
	tg.RLock()
	defer tg.RUnlock()

	if len(tg.targetRing.GetMembers()) == 0 {
		return nil, errors.New("no healthy targets")
	}

	key := []byte(conn.RemoteAddr().String())
	addr := tg.targetRing.LocateKey(key)

	return tg.targets[addr.String()], nil
}

// healthChecks cycles through the target group's targets, checking
// each targets's health every 1 second. If a target is healthy,
// it's added to the target group's hash ring, and if it's unhealthy, it's
// removed.
func (tg *TargetGroup) healthChecks() {
	tg.log.Info("started healthchecks")

	for !tg.shouldStop() {
		tg.RLock()
		targets := tg.targets
		tg.RUnlock()
		for addr, target := range targets {
			healthy := target.healthy()
			// Only add a target to the hash ring if it's healthy *and* still
			// present in the target group config, since this config may have
			// changed concurrently. Otherwise, remove it.
			tg.Lock()
			if _, ok := tg.targets[addr]; ok && healthy {
				tg.targetRing.Add(target.addr)
			} else {
				tg.targetRing.Remove(addr)
			}
			tg.Unlock()
		}

		time.Sleep(1 * time.Second)
	}

	tg.log.Info("stopped healthchecks")
}
