# Introduction

This chart bootstraps a monitoring system on a [Kubernetes]() cluster using the [Helm]() package manager, this chart is used to monitor alluxio cluster.

## Prerequisites

### Kubernetes
Kubernetes 1.11+ with Beta APIs enabled

## Install the Chart

To install the Monitor Chart into your Kubernetes cluster :

```
helm install --namespace "alluxio" --name "monitor" monitor
```

After installation succeeds, you can get a status of Chart

```
helm status "monitor"
```

## Uninstall the Chart

If you want to delete your Chart, use this command:

```
helm delete  --purge "monitor"
```

## Configuration
The monitor chart contains a source directory, some grafana and prometheus config are placed in this directory.
Before installing monitor, make some appropriate modifications.
### 1. source/grafana/datasource.yaml
This grafana datasource url domain name is `[monitor name]-prometheus`, for example: our monitor installation name is alluxio-monitor, then it will be 'alluxio-monitor-prometheus'  
```
http://alluxio-monitor-prometheus:8081  
```
### 2. source/prometheus/prometheus.yaml
Change each prometheus job's namespace, For example, our alluxio cluster is installed in `alluxio` namespace, then edit the prometheus.yml part:
```
namespaces:
  names:
    - alluio
```
### 3. Enable the alluxio metrics
To use the monitor, we need the alluxio prometheus podAnnotations defined in the '../alluxio/values.yaml' metrics part, so it is necessary to set metrics enable true before installing alluxio.
After that, the monitor can keep track of the target alluxio cluster.
## Helm Chart Values

Full documentation can be found in the comments of the `values.yaml` file, but a high level overview is provided here.

__Common Values:__

| Parameter               | Description                    | Default                                 |
|-------------------------|--------------------------------|-----------------------------------------|
| `fullnameOverride`      | To replace the generated name  | `alluxio-monitor`                       |
| `imagePullPolicy`       | Docker image pull policy       | `IfNotPresent`                          |
| `grafanaConfig.name[0]` | Grafana dashboard config name  | `grafana-dashboard-config`              |
| `grafanaConfig.path[0]` | Grafana dashboard config path  | `/etc/grafana/provisioning/dashboards`  |
| `grafanaConfig.name[1]` | Grafana datasource config name | `grafana-datasource-config`             |
| `grafanaConfig.path[1]` | Grafana datasource config path | `/etc/grafana/provisioning/datasources` |
| `prometheusConfig.name` | Prometheus config name         | `prometheus-config`                     |
| `prometheusConfig.path` | Prometheus config path         | `/etc/prometheus`                       |

__Prometheus values:__

| Parameter                   | Description                                                                                                     | Default                                                                                                                                |
|-----------------------------|-----------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `imageInfo.image`           | The prometheus docker image                                                                                     | `prom/prometheus`                                                                                                                      |
| `imageInfo.tag`             | The prometheus image tag                                                                                        | `latest`                                                                                                                               |
| `port.TCP`                  | The prometheus listen address                                                                                   | `9090`                                                                                                                                 |
| `args`                      | The prometheus args                                                                                             | `--config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus --storage.tsdb.retention=72h --web.listen-address=:9090` |
| `hostNetwork`               | Controls whether the pod may use the node network namespace                                                     | `false`                                                                                                                                |
| `dnsPolicy`                 | `dnsPolicy` will be `ClusterFirstWithHostNet` if `hostNetwork: true` and `ClusterFirst` if `hostNetwork: false` | `ClusterFirst`                                                                                                                         |
| `resources.limits.cpu`      | CPU Limit                                                                                                       | `4`                                                                                                                                    |
| `resources.limits.memory`   | Memory Limit                                                                                                    | `4G`                                                                                                                                   |
| `resources.requests.cpu`    | CPU Request                                                                                                     | `1`                                                                                                                                    |
| `resources.requests.memory` | Memory Request                                                                                                  | `1G`                                                                                                                                   |

__Grafana values:__

| Parameter                       | Description                                                                                                     | Default           |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------|-------------------|
| `imageInfo.image`               | The grafana docker image                                                                                        | `grafana/grafana` |
| `imageInfo.tag`                 | The grafana image tag                                                                                           | `latest`          |
| `env.GF_AUTH_BASIC_ENABLED`     | Environment variable of grafana to enable basic authentication                                                  | `true`            |
| `env.GF_AUTH_ANONYMOUS_ENABLED` | Environment variable of grafana to disable anonymous authentication                                             | `false`           |
| `port.web`                      | The grafana web port                                                                                            | `9090`            |
| `port.hostPort`                 | The hostPort export node port to visit the grafana web                                                          | `8081`            |
| `hostNetwork`                   | Controls whether the pod may use the node network namespace                                                     | `false`           |
| `dnsPolicy`                     | `dnsPolicy` will be `ClusterFirstWithHostNet` if `hostNetwork: true` and `ClusterFirst` if `hostNetwork: false` | `ClusterFirst`    |
| `resources.limits.cpu`          | CPU Limit                                                                                                       | `2`               |
| `resources.limits.memory`       | Memory Limit                                                                                                    | `2G`              |
| `resources.requests.cpu`        | CPU Request                                                                                                     | `0.5`             |
| `resources.requests.memory`     | Memory Request                                                                                                  | `1G`              |