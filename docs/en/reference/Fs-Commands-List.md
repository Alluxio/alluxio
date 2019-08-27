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

{% if cmd.options.size > 0 %}

Optionss:
{% for opt in cmd.options %}
- {{opt}}
{% endfor %}

{% endif %}

{{cmd.example}}
  
{% endfor %}
