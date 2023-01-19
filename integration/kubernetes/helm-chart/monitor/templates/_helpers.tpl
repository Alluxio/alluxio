{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "monitor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "monitor.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "monitor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "monitor.prometheus.resources" -}}
resources:
  limits:
    {{- if .Values.prometheus.resources.limits }}
      {{- if .Values.prometheus.resources.limits.cpu  }}
    cpu: {{ .Values.prometheus.resources.limits.cpu }}
      {{- end }}
      {{- if .Values.prometheus.resources.limits.memory  }}
    memory: {{ .Values.prometheus.resources.limits.memory }}
      {{- end }}
    {{- end }}
  requests:
    {{- if .Values.prometheus.resources.requests }}
      {{- if .Values.prometheus.resources.requests.cpu  }}
    cpu: {{ .Values.prometheus.resources.requests.cpu }}
      {{- end }}
      {{- if .Values.prometheus.resources.requests.memory  }}
    memory: {{ .Values.prometheus.resources.requests.memory }}
      {{- end }}
    {{- end }}
{{- end -}}

{{- define "monitor.grafana.readinessProbe" -}}
readinessProbe:
  httpGet:
    path: /login
    port: 3000
  initialDelaySeconds: 30
{{- end -}}

{{- define "monitor.grafana.resources" -}}
resources:
  limits:
    {{- if .Values.grafana.resources.limits }}
      {{- if .Values.grafana.resources.limits.cpu  }}
    cpu: {{ .Values.grafana.resources.limits.cpu }}
      {{- end }}
      {{- if .Values.grafana.resources.limits.memory  }}
    memory: {{ .Values.grafana.resources.limits.memory }}
      {{- end }}
    {{- end }}
  requests:
    {{- if .Values.grafana.resources.requests }}
      {{- if .Values.grafana.resources.requests.cpu  }}
    cpu: {{ .Values.grafana.resources.requests.cpu }}
      {{- end }}
      {{- if .Values.grafana.resources.requests.memory  }}
    memory: {{ .Values.grafana.resources.requests.memory }}
      {{- end }}
    {{- end }}
{{- end -}}
