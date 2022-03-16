# tinylb

A small but mighty TCP load balancer written in Go.

### Features

**Zero-downtime configuration changes**  
Update the load balancer without interruptions to any existing connections. See `config.json` for an example config.

**Sticky sessions**  
Clients are assigned stably to targets using consistent hashing.

**Multiple target groups**  
Distribute load for many services via one load balancer instance - a barebones analogue to AWS ELB's [target group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-target-groups.html) construct.

**Graceful shutdown**  
Safely terminate the load balancer without interruption to open connections.

## Installation

Install the latest release from GitHub.

```bash
go install -v github.com/jacoblurye/tinylb/cmd/tinylb@latest
```

## Usage

Start a load balancer.

```bash
tinylb -config-path config.json
```

Update the load balancer config on the fly.

```bash
curl -X POST --data @../config.json http:/localhost:$CONTROL_PLANE_PORT/config
```
