---
layout: global
title: List of Commands
group: Reference
priority: 0
---
* Table of Contents
{:toc}

## Fs Commands

{% assign sorted_cmds = site.data.table.en.cli.fs | sort %}
{% for item in sorted_cmds %}
{% assign cmd = item[1] %}

---
#### {{cmd.name}} 
  
Usage: `{{cmd.usage}}`
  
{{cmd.description}}

{% if cmd.options %}
Options:
{{cmd.options}}
{% endif %}

{{cmd.example}}
  
{% endfor %}


## Fsadmin Commands

{% assign sorted_admin = site.data.table.en.cli.fsadmin | sort %}
{% for item in sorted_admin %}
{% assign adminCmd = item[1] %}

---
#### {{adminCmd.name}} 
  
Usage: `{{adminCmd.usage}}`
  
{{adminCmd.description}}

{% if adminCmd.options %}
Options:
{{adminCmd.options}}
{% endif %}

{% if adminCmd.subCommands %}
SubCommands:
{{adminCmd.subCommands}}
{% endif %}

{{adminCmd.example}} 
{% endfor %}