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
  
Usages: `{{cmd.Usage}}`
  
{{cmd.Description}}

{% if cmd.Options %}
Options:
{{cmd.Options}}
{% endif %}

{{cmd.Examples}}
  
{% endfor %}