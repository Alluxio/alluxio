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

{{- define "alluxio.proxy.resources" -}}
resources:
  limits:
    {{- if .Values.proxy.resources.limits }}
      {{- if .Values.proxy.resources.limits.cpu  }}
    cpu: {{ .Values.proxy.resources.limits.cpu }}
      {{- end }}
      {{- if .Values.proxy.resources.limits.memory  }}
    memory: {{ .Values.proxy.resources.limits.memory }}
      {{- end }}
    {{- end }}
  requests:
    {{- if .Values.proxy.resources.requests }}
      {{- if .Values.proxy.resources.requests.cpu  }}
    cpu: {{ .Values.proxy.resources.requests.cpu }}
      {{- end }}
      {{- if .Values.proxy.resources.requests.memory  }}
    memory: {{ .Values.proxy.resources.requests.memory }}
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

{{- define "alluxio.logserver.resources" -}}
resources:
  limits:
    {{- if .Values.logserver.resources.limits }}
      {{- if .Values.logserver.resources.limits.cpu  }}
    cpu: {{ .Values.logserver.resources.limits.cpu }}
      {{- end }}
      {{- if .Values.logserver.resources.limits.memory  }}
    memory: {{ .Values.logserver.resources.limits.memory }}
      {{- end }}
    {{- end }}
  requests:
    {{- if .Values.logserver.resources.requests }}
      {{- if .Values.logserver.resources.requests.cpu  }}
    cpu: {{ .Values.logserver.resources.requests.cpu }}
      {{- end }}
      {{- if .Values.logserver.resources.requests.memory  }}
    memory: {{ .Values.logserver.resources.requests.memory }}
      {{- end }}
    {{- end }}
{{- end -}}

{{- define "alluxio.journal.format.resources" -}}
resources:
  limits:
    {{- if .Values.journal.format.resources.limits }}
      {{- if .Values.journal.format.resources.limits.cpu  }}
    cpu: {{ .Values.journal.format.resources.limits.cpu }}
      {{- end }}
      {{- if .Values.journal.format.resources.limits.memory  }}
    memory: {{ .Values.journal.format.resources.limits.memory }}
      {{- end }}
    {{- end }}
  requests:
    {{- if .Values.journal.format.resources.requests }}
      {{- if .Values.journal.format.resources.requests.cpu  }}
    cpu: {{ .Values.journal.format.resources.requests.cpu }}
      {{- end }}
      {{- if .Values.journal.format.resources.requests.memory  }}
    memory: {{ .Values.journal.format.resources.requests.memory }}
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

{{- define "alluxio.logserver.secretVolumeMounts" -}}
  {{- range $key, $val := .Values.secrets.logserver }}
          - name: secret-{{ $key }}-volume
            mountPath: /secrets/{{ $val }}
            readOnly: true
  {{- end -}}
{{- end -}}

{{- define "alluxio.master.configmapVolumeMounts" -}}
  {{- range $key, $val := .Values.configmaps.master }}
            - name: configmap-{{ $key }}-volume
              mountPath: /configmaps/{{ $val }}
              readOnly: true
  {{- end }}
{{- end -}}

{{- define "alluxio.worker.configmapVolumeMounts" -}}
  {{- range $key, $val := .Values.configmaps.worker }}
            - name: configmap-{{ $key }}-volume
              mountPath: /configmaps/{{ $val }}
              readOnly: true
  {{- end -}}
{{- end -}}

{{- define "alluxio.logserver.configmapVolumeMounts" -}}
  {{- range $key, $val := .Values.configmaps.logserver }}
          - name: configmap-{{ $key }}-volume
            mountPath: /configmaps/{{ $val }}
            readOnly: true
  {{- end -}}
{{- end -}}

{{- define "alluxio.master.otherVolumeMounts" -}}
  {{- range .Values.mounts }}
            - name: "{{ .name }}"
              mountPath: "{{ .path }}"
  {{- end }}
{{- end -}}

{{- define "alluxio.worker.otherVolumeMounts" -}}
  {{- range .Values.mounts }}
            - name: "{{ .name }}"
              mountPath: "{{ .path }}"
  {{- end }}
{{- end -}}

{{- define "alluxio.worker.tieredstoreVolumeMounts" -}}
  {{- if .Values.tieredstore.levels }}
    {{- range .Values.tieredstore.levels }}
      {{- /* The mediumtype can have multiple parts like MEM,SSD */}}
      {{- if .mediumtype }}
        {{- /* Mount each part */}}
        {{- if contains "," .mediumtype }}
          {{- $type := .type }}
          {{- $path := .path }}
          {{- $parts := splitList "," .mediumtype }}
          {{- range $i, $val := $parts }}
            {{- /* Example: For path="/tmp/mem,/tmp/ssd", mountPath resolves to /tmp/mem and /tmp/ssd */}}
            - mountPath: {{ index ($path | splitList ",") $i }}
              name: {{ $val | lower }}-{{ $i }}
          {{- end}}
        {{- /* The mediumtype is a single value. */}}
        {{- else}}
            - mountPath: {{ .path }}
              name: {{ .mediumtype | replace "," "-" | lower }}
        {{- end}}
      {{- end}}
    {{- end}}
  {{- end}}
{{- end -}}

{{- define "alluxio.worker.tieredstoreVolumes" -}}
  {{- if .Values.tieredstore.levels }}
    {{- range .Values.tieredstore.levels }}
      {{- if .mediumtype }}
        {{- /* The mediumtype can have multiple parts like MEM,SSD */}}
        {{- if contains "," .mediumtype }}
          {{- $parts := splitList "," .mediumtype }}
          {{- $type := .type }}
          {{- $path := .path }}
          {{- $volumeName := .name }}
          {{- /* A volume will be generated for each part */}}
          {{- range $i, $val := $parts }}
            {{- /* Example: For mediumtype="MEM,SSD", mediumName resolves to mem-0 and ssd-1 */}}
            {{- $mediumName := printf "%v-%v" (lower $val) $i }}
            {{- if eq $type "hostPath"}}
        - hostPath:
            path: {{ index ($path | splitList ",") $i }}
            type: DirectoryOrCreate
          name: {{ $mediumName }}
            {{- else if eq $type "persistentVolumeClaim" }}
        - name: {{ $mediumName }}
          persistentVolumeClaim:
            {{- /* Example: For volumeName="/tmp/mem,/tmp/ssd", claimName resolves to /tmp/mem and /tmp/ssd */}}
            claimName: {{ index ($volumeName | splitList ",") $i }}
            {{- else }}
        - name: {{ $mediumName }}
          emptyDir:
            medium: "Memory"
              {{- if .quota }}
            sizeLimit: {{ .quota }}
              {{- end}}
            {{- end}}
          {{- end}}
        {{- /* The mediumtype is a single value like MEM. */}}
        {{- else}}
          {{- $mediumName := .mediumtype | lower }}
          {{- if eq .type "hostPath"}}
        - hostPath:
            path: {{ .path }}
            type: DirectoryOrCreate
          name: {{ $mediumName }}
          {{- else if eq .type "persistentVolumeClaim" }}
        - name: {{ $mediumName }}
          persistentVolumeClaim:
            claimName: {{ .name }}
          {{- else }}
        - name: {{ $mediumName }}
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

{{- define "alluxio.worker.shortCircuit.volume" -}}
  {{- if eq .Values.shortCircuit.volumeType "hostPath" }}
        - name: alluxio-domain
          hostPath:
            path: {{ .Values.shortCircuit.hostPath }}
            type: DirectoryOrCreate
  {{- else }}
        - name: alluxio-domain
          persistentVolumeClaim:
            claimName: "{{ .Values.shortCircuit.pvcName }}"
  {{- end }}
{{- end -}}

{{- define "alluxio.logserver.log.volume" -}}
{{- if eq .Values.logserver.volumeType "hostPath" }}
- name: alluxio-logs
  hostPath:
    path: {{ .Values.logserver.hostPath }}
    type: DirectoryOrCreate
{{- else if eq .Values.logserver.volumeType "emptyDir" }}
- name: alluxio-logs
  emptyDir:
    medium: {{ .Values.logserver.medium }}
    sizeLimit: {{ .Values.logserver.size | quote }}
{{- else }}
- name: alluxio-logs
  persistentVolumeClaim:
    claimName: "{{ .Values.logserver.pvcName }}"
{{- end }}
{{- end -}}

{{- define "alluxio.hostAliases" -}}
hostAliases:
{{- range .Values.hostAliases }}
- ip: {{ .ip }}
  hostnames:
  {{- range .hostnames }}
  - {{ . }}
  {{- end }}
{{- end }}
{{- end -}}

{{- define "alluxio.imagePullSecrets" -}}
imagePullSecrets:
{{- range $name := .Values.imagePullSecrets }}
  - name: {{ $name }}
{{- end -}}
{{- end -}}

{{/*
Extra volume mounts that can be added to a container
@param .extraVolumeMounts   An object representing a list of volume mounts.
                            Each volumeMount can contain the following fields:
                                volumeMount.name
                                volumeMount.mountPath
                                volumeMount.readOnly
*/}}
{{- define "alluxio.extraVolumeMounts" -}}
  {{- range $volMount := .extraVolumeMounts }}
- name: {{ $volMount.name }}
  mountPath: {{ $volMount.mountPath }}
  readOnly: {{ $volMount.readOnly }}
  {{- end }}
{{- end -}}

{{/*
Extra volumes that can be added to a pod
@param .extraVolumes    An object representing a list of volume mounts.
                        Can use either configMap or emptyDir.
                        Each volume can contain the following fields:
                            volume.name
                            volume.configMap.defaultMode
                            volume.configMap.name
                            volume.emptyDir
*/}}
{{- define "alluxio.extraVolumes" -}}
  {{- range $vol := .extraVolumes }}
- name: {{ $vol.name }}
  {{- if $vol.configMap }}
  configMap:
    {{- if $vol.configMap.defaultMode }}
    defaultMode: {{ $vol.configMap.defaultMode }}
    {{- end}}
    {{- if $vol.configMap.name }}
    name: {{ $vol.configMap.name }}
    {{- end}}
  {{- else }}
  emptyDir: {{ $vol.emptyDir | default "{}" }}
  {{- end }}
  {{- end }}
{{- end -}}

{{/*
Extra ports that can be added to a service
@param .extraServicePorts   An object representing a list of ports.
                            Each item can contain the following fields:
                              item.port
                              item.name
*/}}
{{- define "alluxio.extraServicePorts" -}}
  {{- range $item := .extraServicePorts }}
- port: {{ $item.port }}
  name: {{ $item.name }}
  {{- end -}}
{{- end -}}

{{/*
Extra container specs that can be added to a pod
@param .extraContainers   An object representing a list of containers.
                          Each container can contain the following fields:
                            container.name
                            container.image
                            container.imagePullPolicy
                            container.securityContext.runAsUser
                            container.securityContext.runAsGroup
                            container.resources.limit.cpu
                            container.resources.limit.memory
                            container.resources.requests.cpu
                            container.resources.requests.memory
                            container.command
                            container.args
                            container.env
                            container.envValueFrom
                            container.envFrom
                            container.ports
                            container.volumeMounts
*/}}
{{- define "alluxio.extraContainers" -}}
  {{- range $container := .extraContainers }}
- name: {{ $container.name }}
  image: {{ $container.image }}
  imagePullPolicy: {{ $container.imagePullPolicy }}
  {{- if $container.securityContext }}
  securityContext:
    runAsUser: {{ $container.securityContext.runAsUser }}
    {{- if $container.securityContext.runAsGroup }}
    runAsGroup: {{ $container.securityContext.runAsGroup }}
    {{- end }}
  {{- end }}
  {{- if $container.resources }}
  resources:
    limits:
      {{- if $container.resources.limits }}
        {{- if $container.resources.limits.cpu  }}
      cpu: {{ $container.resources.limits.cpu }}
        {{- end }}
        {{- if $container.resources.limits.memory  }}
      memory: {{ $container.resources.limits.memory }}
        {{- end }}
      {{- end }}
    requests:
      {{- if $container.resources.requests }}
        {{- if $container.resources.requests.cpu  }}
      cpu: {{ $container.resources.requests.cpu }}
        {{- end }}
        {{- if $container.resources.requests.memory  }}
      memory: {{ $container.resources.requests.memory }}
        {{- end }}
      {{- end }}
  {{- end }}
  {{- if $container.command }}
  command:
{{ toYaml $container.command | trim | indent 4 }}
  {{- end }}
  {{- if $container.args }}
  args:
{{ toYaml $container.args | trim | indent 4 }}
  {{- end }}
  {{- if $container.env }}
  env:
    {{- range $key, $value := $container.env }}
    - name: "{{ $key }}"
      value: "{{ $value }}"
    {{- end }}
    {{- if $container.envValueFrom }}
      {{- range $key, $value := $container.envValueFrom }}
    - name: "{{ $key }}"
      valueFrom:
        fieldRef:
          fieldPath: {{ $value }}
      {{- end }}
    {{- end }}
  {{- end }}
  {{- if $container.envFrom }}
  envFrom:
    {{- range $item := $container.envFrom }}
    - configMapRef:
        name: {{ $item.configMapRef.name }}
    {{- end }}
  {{- end }}
  {{- if $container.ports }}
  ports:
    {{- range $port := $container.ports }}
    - containerPort: {{ $port.containerPort }}
      name: {{ $port.name }}
    {{- end }}
  {{- end }}
  {{- if $container.volumeMounts }}
  volumeMounts:
    {{- range $volMount := $container.volumeMounts }}
    - name: {{ $volMount.name }}
      mountPath: {{ $volMount.mountPath }}
      readOnly: {{ $volMount.readOnly }}
      subPath: {{ $volMount.subPath | default "" }}
    {{- end }}
  {{- end }}
  {{- end }}
{{- end -}}
