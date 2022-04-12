package tinylb

import (
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
)

type LoadBalancer struct {
	sync.RWMutex
	Config

	// configPath is the path to the load balancer's config file.
	configPath string
	// stop signals that all long-running goroutines for this target group should stop.
	stop bool
	// closedWatcher signals that the config watcher has exited.
	closedWatcher chan bool
	// targetGroups maps target group names to target groups.
	targetGroups map[string]*TargetGroup

	log *logrus.Logger
}

// Open starts a new load balancer with the given config, ready to receive
// incoming TCP connections.
func Open(configPath string, log *logrus.Logger) (*LoadBalancer, error) {
	lb := &LoadBalancer{
		configPath:    configPath,
		closedWatcher: make(chan bool),
		targetGroups:  make(map[string]*TargetGroup),
		log:           log,
	}
	err := lb.ReloadConfig()
	if err != nil {
		return nil, err
	}

	go lb.configWatcher()

	return lb, nil
}

// UpdateConfig applies the given config to this load balancer without downtime
// for existing or new connections.
func (lb *LoadBalancer) UpdateConfig(config Config) error {
	lb.Lock()
	defer lb.Unlock()

	lb.log.Info("updating config")

	// Find target groups that need to be created.
	create := make([]TargetGroupConfig, 0)
	targetGroupConfigs := make(map[string]TargetGroupConfig, len(config.TargetGroups))
	for _, targetGroupConfig := range config.TargetGroups {
		if _, ok := lb.targetGroups[targetGroupConfig.Name]; !ok {
			create = append(create, targetGroupConfig)
		}
		targetGroupConfigs[targetGroupConfig.Name] = targetGroupConfig
	}

	// Find target groups that need to be updated or removed.
	update := make([]*TargetGroup, 0)
	remove := make([]*TargetGroup, 0)
	for _, target := range lb.targetGroups {
		if _, ok := targetGroupConfigs[target.Name]; ok {
			update = append(update, target)
		} else {
			remove = append(remove, target)
		}
	}

	// Open new target groups.
	for _, targetConfig := range create {
		target, err := openTargetGroup(targetConfig, lb.log)
		if err != nil {
			return err
		}
		lb.targetGroups[target.Name] = target
	}

	// Update existing target groups.
	for _, targetGroup := range update {
		targetGroup := lb.targetGroups[targetGroup.Name]
		targetGroupConfig := targetGroupConfigs[targetGroup.Name]
		err := targetGroup.updateConfig(targetGroupConfig)
		if err != nil {
			return err
		}
	}

	// Remove and drain target groups that are absent from the config.
	var wg sync.WaitGroup
	for _, targetGroup := range remove {
		wg.Add(1)
		go func(tg *TargetGroup) {
			tg.drain()
			wg.Done()
		}(lb.targetGroups[targetGroup.Name])
		delete(lb.targetGroups, targetGroup.Name)
	}
	wg.Wait()

	lb.log.Info("done updating config")

	return nil
}

// Close gracefully shuts down this load balancer, draining all load balancer target
// groups and shutting down the control plane server if it's running.
func (lb *LoadBalancer) Close() error {
	lb.Lock()
	if lb.stop {
		lb.Unlock()
		lb.log.Info("already closed - noop")
		return nil
	}
	lb.stop = true
	lb.Unlock()

	lb.log.Info("closing config watcher")
	<-lb.closedWatcher
	lb.log.Info("config watcher closed")

	lb.Lock()
	defer lb.Unlock()

	lb.log.Info("draining all targets")
	var wg sync.WaitGroup
	for _, targetGroup := range lb.targetGroups {
		wg.Add(1)
		func(tg *TargetGroup) {
			defer wg.Done()
			tg.drain()
		}(targetGroup)
	}
	wg.Wait()
	lb.log.Info("done draining all targets")

	return nil
}

func (lb *LoadBalancer) ReloadConfig() error {
	configFile, err := os.Open(lb.configPath)
	if err != nil {
		return err
	}
	defer configFile.Close()

	config, err := LoadConfig(configFile)
	if err != nil {
		return err
	}

	err = lb.UpdateConfig(*config)
	if err != nil {
		return err
	}

	return nil
}

// configWatcher listens for config file changes at the load balancer's configPath.
// Adapted from: https://martensson.io/go-fsnotify-and-kubernetes-configmaps
func (lb *LoadBalancer) configWatcher() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		lb.log.Panicln(err)
	}
	defer watcher.Close()
	err = watcher.Add(lb.configPath)
	if err != nil {
		lb.log.Errorln("error watching config path: ", err)
	}
	for {
		lb.RLock()
		shouldExit := lb.stop
		lb.RUnlock()
		if shouldExit {
			break
		}

		select {
		case <-time.After(1 * time.Second):
			continue
		case event := <-watcher.Events:
			var err error
			// Support k8s configmap updates, which look like removals.
			if event.Op&fsnotify.Remove == fsnotify.Remove {
				watcher.Remove(event.Name)
				watcher.Add(lb.configPath)
				err = lb.ReloadConfig()
			}
			// Support normal file updates.
			if event.Op&fsnotify.Write == fsnotify.Write {
				err = lb.ReloadConfig()
			}
			if err != nil {
				lb.log.Error("error reloading config: ", err)
			}
		case err := <-watcher.Errors:
			lb.log.Error("error watching config: ", err)
		}
	}

	lb.closedWatcher <- true
}
