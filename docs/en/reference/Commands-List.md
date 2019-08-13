<table>
{% for item in site.data.table.en.cli %}
  <tr>
    <td><a class="anchor" name="{{ item.propertyName }}"></a> {{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.common-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>