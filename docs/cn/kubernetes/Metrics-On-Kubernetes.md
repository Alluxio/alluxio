---
布局: 全局
标题: Kubernetes 的 Metrics 系统
---

关于 Alluxio metrics （指标）的基本信息，请参考 [Metrics System]({{ '/en/operation/Metrics-System.html' | relativize_url }}).
关于如何在 Kubernetes 中部署 Alluxio, 请参考 [在 Kubernetes 中安装 Alluxio]({{ '/en/kubernetes/Install-Alluxio-On-Kubernetes.html' | relativize_url }}).

本文档主要描述如何为 Kubernetes 集群中的 Alluxio 配置和获取各种 metrics sink 中的 metrics。 

## HTTP JSON Sink

### 配置

Metrics 通过 Alluxio 各个组件的 web 端口对外暴露。

* Alluxio masters 和 workers 的 web 端口默认情况下都是打开的。
* Alluxio standalone Fuse 的 web 端口默认是关闭的，可以通过配置 `alluxio.fuse.web.enabled` 为 true 来打开该端口.

### 获取 Metrics 快照

用户可以向 Alluxio 进程发送 HTTP 请求，以获取 JSON 格式的 metrics 快照。
```shell
# 获取某特定组件的 JSON 格式的 metrics:
$ kubectl exec <COMPONENT_HOSTNAME> -c <CONTAINER_NAME> -- curl 127.0.0.1:<COMPONENT_WEB_PORT>/metrics/json/
```

```shell
# 比如，从默认 web 端口 19999 获取 leading master 组件的 metrics:
$ kubectl exec <alluxio-master-x> -c alluxio-master -- curl 127.0.0.1:19999/metrics/json/
```

```shell
# 从默认 web 端口 30000 获取 worker 组件的 metrics:
$ kubectl exec <alluxio-worker-xxxxx> -c alluxio-worker -- curl 127.0.0.1:30000/metrics/json/
```

```shell
# 从默认 web 端口 20002 获取 leading job master 组件的 metrics:
$ kubectl exec <alluxio-master-x> -c alluxio-job-master -- curl 127.0.0.1:20002/metrics/json/
```

```shell
# 从默认 web 端口 30003 获取 job worker 组件的 metrics:
$ kubectl exec <alluxio-worker-xxxxx> -c alluxio-job-worker -- curl 127.0.0.1:30003/metrics/json/
```

```shell
# 从默认 web 端口 49999 获取 fuse 进程的 metrics:
$ kubectl exec <alluxio-fuse-xxxxx> -- curl 127.0.0.1:49999/metrics/json/
```

## Prometheus Sink

[Prometheus](https://prometheus.io/) 作为一款监控工具，可以监控 Alluxio metrics 的变化。

### 配置 Alluxio Helm Chart

在 Alluxio 中需要开启 Prometheus 的 `PrometheusMetricsServlet`。要开启 Prometheus metrics sink，需要在 helm chart `value.yaml` 中配置如下参数：
```yaml
metrics:
  enabled: true
  PrometheusMetricsServlet: true
  podAnnotations:
    prometheus.io/scrape: "true"
    prometheus.io/masterWebPort: "<MASTER_WEB_PORT>"
    prometheus.io/jobMasterWebPort: "<JOB_MASTER_WEB_PORT>"
    prometheus.io/workerWebPort: "<WORKER_WEB_PORT>"
    prometheus.io/jobWorkerWebPort: "<JOB_WORKER_WEB_PORT>"
    prometheus.io/fuseWebPort: "<FUSE_WEB_PORT>"
    prometheus.io/path: "/metrics/prometheus/"
```

类似 HTTP JSON Sink，要访问 fuse web 的 metrics，需要通过配置 `alluxio.fuse.web.enabled` 为 true 来打开其 web 端口。

### 配置 Prometheus 客户端

要使用 Prometheus 客户端从 Alluxio 中获取 metrics, 需要配置 Prometheus 客户端的 `prometheus.yml`. 比如，获取 master metrics 时，进行如下配置:

```yaml
scrape_configs:
  - job_name: 'alluxio master'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - alluxio # 只访问命名空间 `alluxio` 中的 pod
    relabel_configs:
      # 只访问角色为 `alluxio-master` 的 pod
      - source_labels: [__meta_kubernetes_pod_label_role]
        action: keep
        regex: alluxio-master
      # 只访问注解 prometheus.io/scrape 为 true 的 pods 
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      # 使用 podAnnotation 中 prometheus.io/path 的值作为访问点
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        action: replace
        target_label: __metrics_path__
        regex: (.+)
      # 使用 podAnnotation 中 prometheus.io/masterWebPort 的值作为访问端口
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_masterWebPort]
        action: replace
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
        target_label: __address__
      - action: labelmap
        regex: __meta_kubernetes_pod_label_(.+)
      - source_labels: [__meta_kubernetes_namespace]
        action: replace
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        action: replace
        target_label: pod_name
      - source_labels: [__meta_kubernetes_pod_node_name]
        action: replace
        target_label: node
      - source_labels: [__meta_kubernetes_pod_label_release]
        action: replace
        target_label: cluster_name
```

要获取其它组件的 metrics, 需要配置各自对应的 pod 角色标签和网络端口。

#### Worker metrics
获取 worker metrics 的示例配置：
```yaml
scrape_configs:
  - job_name: 'alluxio worker'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - alluxio # 只访问命名空间 `alluxio` 中的 pod
    relabel_configs:
      # 只访问角色为 `alluxio-worker` 的 pod
      - source_labels: [__meta_kubernetes_pod_label_role]
        action: keep
        regex: alluxio-worker
      # 只访问注解 prometheus.io/scrape 为 true 的 pods
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      # 使用 podAnnotation 中 prometheus.io/path 的值作为访问点
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        action: replace
        target_label: __metrics_path__
        regex: (.+)
      # 使用 podAnnotation 中 prometheus.io/workerWebPort 的值作为访问端口
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_workerWebPort]
        action: replace
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
        target_label: __address__
      - action: labelmap
        regex: __meta_kubernetes_pod_label_(.+)
      - source_labels: [__meta_kubernetes_namespace]
        action: replace
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        action: replace
        target_label: pod_name
      - source_labels: [__meta_kubernetes_pod_node_name]
        action: replace
        target_label: node
      - source_labels: [__meta_kubernetes_pod_label_release]
        action: replace
        target_label: cluster_name
```

#### Job master metrics 
获取 job master metrics 的示例配置：
```yaml
scrape_configs:
  - job_name: 'alluxio job worker'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - alluxio # 只访问命名空间 `alluxio` 中的 pod
    relabel_configs:
      #  只访问角色为 `alluxio-master` 的 pod
      - source_labels: [__meta_kubernetes_pod_label_role]
        action: keep
        regex: alluxio-master
      # 只访问注解 prometheus.io/scrape 为 true 的 pods
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      # 使用 podAnnotation 中 prometheus.io/path 的值作为访问点
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        action: replace
        target_label: __metrics_path__
        regex: (.+)
      # 使用 podAnnotation 中 prometheus.io/jobMasterWebPort 的值作为访问端口
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_jobMasterWebPort]
        action: replace
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
        target_label: __address__
      - action: labelmap
        regex: __meta_kubernetes_pod_label_(.+)
      - source_labels: [__meta_kubernetes_namespace]
        action: replace
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        action: replace
        target_label: pod_name
      - source_labels: [__meta_kubernetes_pod_node_name]
        action: replace
        target_label: node
      - source_labels: [__meta_kubernetes_pod_label_release]
        action: replace
        target_label: cluster_name
```

#### Job worker metrics 
获取 job worker metrics 的示例配置：
```yaml
scrape_configs:
  - job_name: 'alluxio job worker'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - alluxio # 只访问命名空间 `alluxio` 中的 pod
    relabel_configs:
      # 只访问角色为 `alluxio-worker` 的 pod
      - source_labels: [__meta_kubernetes_pod_label_role]
        action: keep
        regex: alluxio-worker
      # 只访问注解 prometheus.io/scrape 为 true 的 pods
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      # 使用 podAnnotation 中 prometheus.io/path 的值作为访问点
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        target_label: __metrics_path__
        regex: (.+)
      # 使用 podAnnotation 中 prometheus.io/jobWorkerWebPort 的值作为访问端口
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_jobWorkerWebPort]
        action: replace
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
        target_label: __address__
      - action: labelmap
        regex: __meta_kubernetes_pod_label_(.+)
      - source_labels: [__meta_kubernetes_namespace]
        action: replace
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        action: replace
        target_label: pod_name
      - source_labels: [__meta_kubernetes_pod_node_name]
        action: replace
        target_label: node
      - source_labels: [__meta_kubernetes_pod_label_release]
        action: replace
        target_label: cluster_name
```

### 获取 Metrics 快照

用户可以向 Alluxio 进程对应的 Prometheus endpoint 发送 HTTP 请求，以获取 Prometheus 格式的 metrics 快照。

```shell
# 获取某特定组件的 Prometheus 格式的 metrics:
$ kubectl exec <COMPONENT_HOSTNAME> -c <CONTAINER_NAME> -- curl 127.0.0.1:<COMPONEMT_WEB_PORT>/metrics/prometheus/
```

```shell
# 比如，从默认 web 端口 19999 获取 leading master 组件的 metrics:
$ kubectl exec <alluxio-master-x> -c alluxio-master -- curl 127.0.0.1:19999/metrics/prometheus/
```

```shell
# 从默认 web 端口 30000 获取 worker 组件的 metrics:
$ kubectl exec <alluxio-worker-xxxxx> -c alluxio-worker -- curl 127.0.0.1:30000/metrics/prometheus/
```

```shell
# 从默认 web 端口 20002 获取 leading job master 组件的 metrics:
$ kubectl exec <alluxio-master-x> -c alluxio-job-master -- curl 127.0.0.1:20002/metrics/prometheus/
```

```shell
# 从默认 web 端口 30003 获取 job worker 组件的 metrics:
$ kubectl exec <alluxio-worker-xxxxx> -c alluxio-job-worker -- curl 127.0.0.1:30003/metrics/prometheus/
```

```shell
# 从默认 web 端口 49999 获取 fuse 进程的 metrics:
$ kubectl exec <alluxio-fuse-xxxxx> -- curl 127.0.0.1:49999/metrics/prometheus/
```
