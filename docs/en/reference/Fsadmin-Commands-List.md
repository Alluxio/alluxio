---
layout: global
title: List of Commands
group: Reference
priority: 0
---
* Table of Contents
{:toc}

## Fsadmin Commands

{% assign sorted_admin = site.data.table.en.cli.fsadmin | sort %}
{% for item in sorted_admin %}
{% assign adminCmd = item[1] %}

---
#### {{adminCmd.name}} 
  
Usage: `{{adminCmd.usage}}`
  
{{adminCmd.description}}

{% if adminCmd.options.size > 0 %}

Options:
{% for opt in adminCmd.options %}
- {{opt}}
{% endfor %}

{% endif %}

{% if adminCmd.subCommands %}
SubCommands:
{{adminCmd.subCommands}}
{% endif %}

{{adminCmd.example}} 
{% endfor %}
