package main

import (
	"flag"
	"os"
	"os/signal"

	tinylb "github.com/jacoblurye/tinylb/tinylb"
	"github.com/sirupsen/logrus"
)

var (
	log        = logrus.New()
	configPath = flag.String("config-path", "", "path to a JSON config file")
	logLevel   = flag.String("log-level", "info", "filter logs with severity below this level")
)

func init() {
	flag.Parse()
	if *configPath == "" {
		log.Fatal("error: missing required flag -config-path")
	}
	if level, err := logrus.ParseLevel(*logLevel); err != nil {
		log.Fatal("error: %s", err)
	} else {
		log.SetLevel(level)
	}
}

func main() {
	configFile, err := os.Open(*configPath)
	if err != nil {
		log.Fatal("error loading config: ", err)
	}
	defer configFile.Close()

	config, err := tinylb.LoadConfig(configFile)
	if err != nil {
		log.Fatal("error loading config: ", err)
	}

	lb, err := tinylb.Open(*config, log)
	if err != nil {
		log.Fatal("error opening load balancer: ", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Info("signal: ", os.Interrupt)

	err = lb.Close()
	if err != nil {
		log.Fatal("error closing load balancer: ", err)
	}
}
