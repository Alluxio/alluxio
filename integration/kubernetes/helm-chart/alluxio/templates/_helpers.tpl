{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{/*
Bring the items back
*/}}

{{- define "alluxio.name" -}}
{{- printf "%v" "alluxio" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "alluxio.fullname" -}}
{{- printf "%v" "alluxio-full"}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "alluxio.chart" -}}
{{- printf "%s-%s" "alluxio" "0.4.0" | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
