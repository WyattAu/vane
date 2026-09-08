{{- define "vane.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "vane.fullname" -}}
{{- .Release.Name }}-{{ .Chart.Name -}}
{{- end -}}
