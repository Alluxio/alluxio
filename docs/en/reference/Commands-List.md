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
#### {{cmd.Name}} 
  
Usage: `{{cmd.Usage}}`
  
{{cmd.Description}}

{% if cmd.Options %}
Options:
{{cmd.Options}}
{% endif %}

{{cmd.Examples}}
  
{% endfor %}


## Fsadmin Commands

{% assign sorted_admin = site.data.table.en.cli.fsadmin | sort %}
{% for item in sorted_admin %}
{% assign adminCmd = item[1] %}

---
#### {{adminCmd.Name}} 
  
Usage: `{{adminCmd.Usage}}`
  
{{adminCmd.Description}}

{% if adminCmd.Options %}
Options:
{{adminCmd.Options}}
{% endif %}

{% if adminCmd.SubCommands %}
SubCommands:
{{adminCmd.SubCommands}}
{% endif %}

{{adminCmd.Examples}} 
{% endfor %}