package tinylb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/sirupsen/logrus"
)

type LoadBalancer struct {
	sync.Mutex
	Config

	// controlSrv is the control plane's HTTP server.
	controlSrv *http.Server
	// targetGroups maps target group names to target groups.
	targetGroups map[string]*TargetGroup

	log *logrus.Logger
}

// Open starts a new load balancer with the given config, ready to receive
// incoming TCP connections.
func Open(config Config, log *logrus.Logger) (*LoadBalancer, error) {
	targetGroups := make(map[string]*TargetGroup, len(config.TargetGroups))
	for _, targetGroupConfig := range config.TargetGroups {
		targetGroup, err := openTargetGroup(targetGroupConfig, log)
		if err != nil {
			return nil, err
		}
		targetGroups[targetGroupConfig.Name] = targetGroup
	}
	lb := &LoadBalancer{Config: config, targetGroups: targetGroups, log: log}
	lb.openControlPlane()

	return lb, nil
}

// UpdateConfig applies the given config to this load balancer without downtime
// for existing or new connections.
func (lb *LoadBalancer) UpdateConfig(config Config) error {
	lb.Lock()
	defer lb.Unlock()

	lb.log.Info("updating config")

	if config.ControlPlane.Port != lb.ControlPlane.Port {
		return errors.New("cannot update control plane port on-the-fly")
	}

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

	if lb.controlSrv != nil {
		lb.log.Info("shutting down control plane")
		err := lb.controlSrv.Shutdown(context.Background())
		if err != nil {
			return err
		}
		lb.log.Info("done shutting down control plane")
	}

	return nil
}

// openControlPlane starts the HTTP control plane server for this load balancer.
//
// Currently, this server has a single endpoint `/config`. POST requests to
// this endpoint containing a valid JSON config in their request body will be
// applied to the load balancer.
func (lb *LoadBalancer) openControlPlane() {
	log := lb.log.WithField("control_plane_port", lb.Config.ControlPlane.Port)
	handler := http.NewServeMux()
	handler.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if r.Method != "POST" {
			log.Errorf("%s /config %d method not allowed", r.Method, http.StatusMethodNotAllowed)
			w.WriteHeader(http.StatusMethodNotAllowed)
			fmt.Fprintln(w, `{"error":"method not allowed"}`)
			return
		}

		config, err := LoadConfig(r.Body)
		if err != nil {
			log.Errorf("%s /config %d %s", r.Method, http.StatusUnprocessableEntity, err)
			w.WriteHeader(http.StatusUnprocessableEntity)
			fmt.Fprintf(w, `{"error":"%s"}`, err)
			return
		}

		err = lb.UpdateConfig(*config)
		if err != nil {
			log.Errorf("%s /config %d %s", r.Method, http.StatusBadRequest, err)
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, `{"error":"%s"}`, err)
			return
		}

		log.Infof("%s /config %d", r.Method, http.StatusOK)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"ok":true}`)
	})

	lb.controlSrv = &http.Server{Addr: fmt.Sprintf(":%d", lb.Config.ControlPlane.Port), Handler: handler}
	go lb.controlSrv.ListenAndServe()

	log.Info("started control plane")
}
