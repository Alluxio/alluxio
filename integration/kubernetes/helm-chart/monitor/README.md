# Introduction

This chart bootstraps a monitoring system on a Kubernetes cluster using the [Helm](https://helm.sh/docs/using_helm/#installing-helm) package manager. This monitor system can be used to 
monitor an [Alluxio](https://www.alluxio.io/) cluster started on Kubernetes cluster.

## Pre-requisites

### Kubernetes
Kubernetes 1.11+ with Beta APIs enabled

## Install the Chart

To install the Monitor Chart into your Kubernetes cluster:

```
$ helm install --namespace "alluxio" "alluxio-monitor" monitor
```

After installation succeeds, you can get a status of Chart

```
$ helm status "alluxio-monitor"
```

## Uninstall the Chart

If you want to delete your Chart, use this command:

```
$ helm delete --purge "alluxio-monitor"
```

## Configuration
The monitor system is implemented based on Prometheus + Grafana, the resource files are placed in the `monitor/source` directory.
Before installing the monitor chart, you may make some appropriate modifications to the configuration.
### 1. source/grafana/datasource.yaml
This grafana datasource url domain name is `[MONITORNAME]-prometheus`, for example: our monitor installation name is `alluxio-monitor`, then it will be 'alluxio-monitor-prometheus'  
```
datasources:
  - name: Prometheus
    ...
    url: http://alluxio-monitor-prometheus:9090 
```
### 2. source/prometheus/prometheus.yaml
Change each prometheus job's namespace, For example, if the alluxio cluster we want to monitor is installed in `alluxio` namespace, then edit the prometheus.yaml:
```
scrape_configs:
  - job_name: 'alluxio master'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - alluxio
```
### 3. Enable the alluxio metrics
To use the monitor, we need the alluxio prometheus podAnnotations defined in the '../alluxio/values.yaml' metrics part, so it is necessary to enable metrics before installing the alluxio chart.
After that, the monitor can keep track of the target alluxio cluster.
```
metrics:
  enabled: true
  ...
  PrometheusMetricsServlet:
    enabled: true
  # Pod annotations for Prometheus
  podAnnotations:
     prometheus.io/scrape: "true"
     prometheus.io/port: "19999"
     prometheus.io/jobPort: "20002"
     prometheus.io/workerPort: "30000"
     prometheus.io/path: "/metrics/prometheus/"
```
### 4. Download the alluxio dashboard
Download the alluxio dashboard from [Alluxio grafana dashboard V1](https://grafana.com/grafana/dashboards/17785-alluxio-prometheus-grafana-monitor-v1/), then
move the dashboard file to `monitor/source/grafana/dashboard` directory.

## Helm Chart Values

Full documentation can be found in the comments of the `values.yaml` file, but a high level overview is provided here.

__Common Values:__

| Parameter               | Description                                            | Default                                 |
|-------------------------|--------------------------------------------------------|-----------------------------------------|
| `fullnameOverride`      | To replace the generated name                          | `alluxio-monitor`                       |
| `imagePullPolicy`       | Docker image pull policy                               | `IfNotPresent`                          |
| `grafanaConfig.name[0]` | Grafana dashboard config name                          | `grafana-dashboard-config`              |
| `grafanaConfig.path[0]` | Grafana dashboard config path in the image container   | `/etc/grafana/provisioning/dashboards`  |
| `grafanaConfig.name[1]` | Grafana datasource config name                         | `grafana-datasource-config`             |
| `grafanaConfig.path[1]` | Grafana datasource config path in the image container  | `/etc/grafana/provisioning/datasources` |
| `prometheusConfig.name` | Prometheus config name                                 | `prometheus-config`                     |
| `prometheusConfig.path` | Prometheus config  path in the image container         | `/etc/prometheus`                       |

__Prometheus values:__

| Parameter                   | Description                                                                                                     | Default                                                                                                                                |
|-----------------------------|-----------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `imageInfo.image`           | The prometheus docker image                                                                                     | `prom/prometheus`                                                                                                                      |
| `imageInfo.tag`             | The prometheus image tag                                                                                        | `latest`                                                                                                                               |
| `port.TCP`                  | The prometheus default listen address                                                                           | `9090`                                                                                                                                 |
| `args`                      | The prometheus config args, see values.yaml for detail explanation                                              | `--config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus --storage.tsdb.retention=72h --web.listen-address=:9090` |
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

