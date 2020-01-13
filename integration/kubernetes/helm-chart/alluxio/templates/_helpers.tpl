{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "alluxio.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "alluxio.fullname" -}}
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
{{- define "alluxio.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "alluxio.jobWorker.resources" -}}
resources:
  limits:
    {{- if .Values.jobWorker.resources.limits }}
      {{- if .Values.jobWorker.resources.limits.cpu  }}
    cpu: {{ .Values.jobWorker.resources.limits.cpu }}
      {{- end }}
      {{- if .Values.jobWorker.resources.limits.memory  }}
    memory: {{ .Values.jobWorker.resources.limits.memory }}
      {{- end }}
    {{- end }}
  requests:
    {{- if .Values.jobWorker.resources.requests }}
      {{- if .Values.jobWorker.resources.requests.cpu  }}
    cpu: {{ .Values.jobWorker.resources.requests.cpu }}
      {{- end }}
      {{- if .Values.jobWorker.resources.requests.memory  }}
    memory: {{ .Values.jobWorker.resources.requests.memory }}
      {{- end }}
    {{- end }}
{{- end -}}

{{- define "alluxio.worker.resources" -}}
resources:
  limits:
    {{- if .Values.worker.resources.limits }}
      {{- if .Values.worker.resources.limits.cpu  }}
    cpu: {{ .Values.worker.resources.limits.cpu }}
      {{- end }}
      {{- if .Values.worker.resources.limits.memory  }}
    memory: {{ .Values.worker.resources.limits.memory }}
      {{- end }}
    {{- end }}
  requests:
    {{- if .Values.worker.resources.requests }}
      {{- if .Values.worker.resources.requests.cpu  }}
    cpu: {{ .Values.worker.resources.requests.cpu }}
      {{- end }}
      {{- if .Values.worker.resources.requests.memory  }}
    memory: {{ .Values.worker.resources.requests.memory }}
      {{- end }}
    {{- end }}
{{- end -}}

{{- define "alluxio.master.resources" -}}
resources:
  limits:
    {{- if .Values.master.resources.limits }}
      {{- if .Values.master.resources.limits.cpu  }}
    cpu: {{ .Values.master.resources.limits.cpu }}
      {{- end }}
      {{- if .Values.master.resources.limits.memory  }}
    memory: {{ .Values.master.resources.limits.memory }}
      {{- end }}
    {{- end }}
  requests:
    {{- if .Values.master.resources.requests }}
      {{- if .Values.master.resources.requests.cpu  }}
    cpu: {{ .Values.master.resources.requests.cpu }}
      {{- end }}
      {{- if .Values.master.resources.requests.memory  }}
    memory: {{ .Values.master.resources.requests.memory }}
      {{- end }}
    {{- end }}
{{- end -}}

{{- define "alluxio.jobMaster.resources" -}}
resources:
  limits:
    {{- if .Values.jobMaster.resources.limits }}
      {{- if .Values.jobMaster.resources.limits.cpu  }}
    cpu: {{ .Values.jobMaster.resources.limits.cpu }}
      {{- end }}
      {{- if .Values.jobMaster.resources.limits.memory  }}
    memory: {{ .Values.jobMaster.resources.limits.memory }}
      {{- end }}
    {{- end }}
  requests:
    {{- if .Values.jobMaster.resources.requests }}
      {{- if .Values.jobMaster.resources.requests.cpu  }}
    cpu: {{ .Values.jobMaster.resources.requests.cpu }}
      {{- end }}
      {{- if .Values.jobMaster.resources.requests.memory  }}
    memory: {{ .Values.jobMaster.resources.requests.memory }}
      {{- end }}
    {{- end }}
{{- end -}}

{{- define "alluxio.master.secretVolumeMounts" -}}
  {{- range $key, $val := .Values.secrets.master }}
          - name: secret-{{ $key }}-volume
            mountPath: /secrets/{{ $val }}
            readOnly: true
  {{- end }}
{{- end -}}

{{- define "alluxio.worker.secretVolumeMounts" -}}
  {{- range $key, $val := .Values.secrets.worker }}
            - name: secret-{{ $key }}-volume
              mountPath: /secrets/{{ $val }}
              readOnly: true
  {{- end -}}
{{- end -}}

{{- define "alluxio.worker.tieredstoreVolumeMounts" -}}
  {{- if .Values.tieredstore.levels }}
    {{- range .Values.tieredstore.levels }}
      {{- if .mediumtype }}
        {{- if contains "," .mediumtype }}
{{- $type := .type }}
{{- $path := .path }}
{{- $split := split "," .mediumtype }}
          {{- range $key, $val := $split }}
            {{- if eq $type "hostPath"}}
            - mountPath:  {{ index ($path | split ",") $key }}
              name: {{ $val | lower }}-{{ $key | replace "_" "" }}
            {{- end}}
          {{- end}}
        {{- else}}
            - mountPath: {{ .path }}
              name: {{ .mediumtype | replace "," "-" | lower }}
        {{- end}}
      {{- end}}
    {{- end}}
  {{- end}}
{{- end -}}

{{- define "alluxio.worker.otherVolumeMounts" -}}
  {{- range .Values.mounts }}
            - name: "{{ .name }}"
              mountPath: "{{ .path }}"
  {{- end }}
{{- end -}}

{{- define "alluxio.worker.tieredstoreVolumes" -}}
  {{- if .Values.tieredstore.levels }}
    {{- range .Values.tieredstore.levels }}
      {{- if .mediumtype }}
        {{- if contains "," .mediumtype }}
          {{- $split := split "," .mediumtype }}
          {{- $type := .type }}
          {{- $path := .path }}
          {{- range $key, $val := $split }}
            {{- if eq $type "hostPath"}}
        - hostPath:
            path: {{ index ($path | split ",") $key }}
            type: DirectoryOrCreate
          name: {{ $val | lower }}-{{ $key | replace "_" "" }}
            {{- else }}
        - name: {{ $val | lower }}-{{ $key | replace "_" "" }}
          emptyDir:
            medium: "Memory"
              {{- if .quota }}
            sizeLimit: {{ .quota }}
              {{- end}}
            {{- end}}
          {{- end}}
        {{- else}}
          {{- if eq .type "hostPath"}}
        - hostPath:
            path: {{ .path }}
            type: DirectoryOrCreate
          name: {{ .mediumtype | replace "," "-" | lower }}
          {{- else }}
        - name: {{ .mediumtype | replace "," "-" | lower }}
          emptyDir:
            medium: "Memory"
            {{- if .quota }}
            sizeLimit: {{ .quota }}
            {{- end}}
          {{- end}}
        {{- end}}
      {{- end}}
    {{- end}}
  {{- end}}
{{- end -}}

{{- define "alluxio.worker.secretVolumes" -}}
  {{- range $key, $val := .Values.secrets.worker }}
        - name: secret-{{ $key }}-volume
          secret:
            secretName: {{ $key }}
            defaultMode: 256
  {{- end }}
{{- end -}}
