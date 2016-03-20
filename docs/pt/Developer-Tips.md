---
layout: global
title: Dicas de Desenvolvedor
nickname: Dicas de Desenvolvedor
group: Resources
---

* Table of Contents
{:toc}

Esta página possui uma coleção de dicas e `howtos` construídos pelos desenvolvedores do Alluxio.

### Alterar a definição de um Thrift RPC

O Alluxio utiliza o `thrift` para comunicação `RPC` entre os `clients` e servidores. Os arquivos 
`.thrift`, que estão definidos em `common/src/thrift/`, são utilizados na geração automática de 
códigos `Java` para efetuarem chamadas `RPCs` em `clients` e implementar os `RPCs` nos servidores. 
Para mudar uma definição `Thrift`, primeiramente, você precisa 
[instalar o compilador Thrift](https://thrift.apache.org/docs/install/). Se você possuir o `brew`, 
você pode fazer isso, executando:

{% include Developer-Tips/install-thrift.md %}

Então para regenerar o código `Java`, execute:

{% include Developer-Tips/thriftGen.md %}

### Alterar o Protocol Buffer Message

O Alluxio utiliza `protocol buffers` para ler e escrever mensagens `journal`. Os arquivos `.proto`, 
que estão definidos em `servers/src/proto/journal/`, são utilizados na geração automática de 
definições `Java` para as mensagens `protocol buffer`. Para modificar essas mensagens, primeiro 
leia sobre [atualizar um tipo de mensagem](https://developers.google.com/protocol-buffers/docs/proto#updating) 
para ter certeza que a sua alteração não danifique a compatibilidade com versões anteriores. Em 
seguida, 
[instale o protocol](https://github.com/google/protobuf#protocol-buffers---googles-data-interchange-format).
Se você possuir o `brew`, você pode fazer isso, executando:

{% include Developer-Tips/install-protobuf.md %}

Então para regenerar o código `Java`, execute:

{% include Developer-Tips/protoGen.md %}

### Lista completa dos comandos em bin/alluxio

A maioria dos comandos em `bin/alluxio` são para desenvolvedores. A tabela a seguir explica a descrição e 
a sintaxe de cada comando.

<table class="table table-striped">
<tr><th>Command</th><th>Args</th><th>Description</th></tr>
</tr>
{% for dscp in site.data.table.Developer-Tips %}
<tr>
  <td>{{dscp.command}}</td>
  <td>{{dscp.args}}</td>
  <td>{{site.data.table.en.Developer-Tips.[dscp.command]}}</td>
</tr>
{% endfor %}
</table>

Estes comandos possuem pré-requisitos diferentes. O pré-requisito para os comandos `format`, `formatWorker`, 
`journalCrashTest`, `readJournal`, `version` e `validateConf` é que você tenha 
configurado o Alluxio (veja o [Construindo o Alluxio Master Branch](Building-Alluxio-Master-Branch.html) 
para saber como configurar o Alluxio manualmente). Além disso, o pré-requisito para os comandos `fs`, 
`loadufs`, `runTest` e `runTests` é que você possua o sistema do Alluxio em execução.
