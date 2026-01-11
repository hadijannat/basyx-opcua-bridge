{{- define "basyx-opcua-bridge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "basyx-opcua-bridge.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "basyx-opcua-bridge.name" . -}}
{{- printf "%s" $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "basyx-opcua-bridge.labels" -}}
app.kubernetes.io/name: {{ include "basyx-opcua-bridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "basyx-opcua-bridge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "basyx-opcua-bridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
