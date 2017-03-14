---
layout: global
title: Key Value System Client API
nickname: Key Value System API
group: Features
priority: 4
---

* Table of Contents
{:toc}

## Visão Geral
Além do [Filesystem API](File-System-API.html) que permite as aplicações de ler, escrever ou
gerenciar arquivos, o Alluxio também servir como um sistema `key-value` sobre o Alluxio
`file system`. Assim como nos arquivos do Alluxio `file system`, a semântica no sistema
`key-value` também são escritas uma única vez:

* Os usuários podem criar um armazenamento `key-value` e inserir pares `key-value` no
armazenamento. O armazenamento se torna imutável assim que estiver concluído.
* Os usuários podem abrir o armazenamento `key-value` depois que estiver concluído.

Cada armazenamento único `key-value` é indicado por um `AlluxioURI` como
`alluxio://path/my-kvstore`. Dependendo do tamanho total e do tamanho do bloco utilizado pelo
usuário, um único armazenamento `key-value` pode consistir de uma ou múltiplas partições mas
é gerenciado interamente pelo Alluxio e é transparente para o usuário.

## Acessando o Sistema Key-Value em Aplicações Java

#### Obtendo um Key-Value System Client

Para obter um Alluxio `key-value system client` em código `Java`, execute:

{% include Key-Value-Store-API/get-key-value-system.md %}

### Criando um novo armazenamento key-value

Para criar um novo armazenamento `key-valye`, utilize `KeyValueSystem#createStore(AlluxioURI)`,
que retorna um `escritor` para adicionar o par `key-value`. Por exemplo:

{% include Key-Value-Store-API/create-new-key-value.md %}

Atentem que:

* Antes que o `escritor` feche, o armazenamento não está completo e não pode ser lido;
* É possível que o armazenamento seja maior que o tamanho máximo permitido de uma partição, e
neste caso, o `escritor` vai armazenar os pares `key-value` em múltiplas partições. Esta operação
é transparente.
* As chaves para inserir devem ser ordenadas e não podem possuir valores duplicados.

### Readquirindo valores de um armazenamento

Para buscar um armazenamento `key-value` completo, utilize o `KeyValueSystem#openStore(AlluxioURI)`,
que retorna um `leitor` para buscar o valor por chave. Por exemplo:

{% include Key-Value-Store-API/read-value.md %}

### Interagindo pares key-value sobre um armazenamento

{% include Key-Value-Store-API/iterate-key-values.md %}

## Acessando o Sistema Key-Value dentro do Hadoop MapReduce

### MapReduce InputFormat

O Alluxio provê uma implementação do `InputFormat` para os programas `Hadoop MapReduce` acessarem
o armazenamento `key-value`. Este obtém um `key-value URI` e emite um par `key-value` que está
no armazenamento:

{% include Key-Value-Store-API/set-input-format.md %}


### MapReduce OutputFormat

Similarmente, o Alluxio também provê implementações de `OutputFormat` e `OutputCommitter` para
que programas `Hadoop MapReduce` criem armazenamentos `key-value` através de um `key-value URI`,
e salvem os pares `key-value` em um armazenamento `key-value`:

{% include Key-Value-Store-API/set-output-format.md %}


## Parametros de Configuração para o Sistema Key-Value

O suporte a `key-value` no Alluxio está desabilitado por padrão e pode ser habilitado no
Alluxio através da alteração do parametro `alluxio.keyvalue.enabled` para `true`
(veja [os parametros de configuração](Configuration-Settings.html))

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.key-value-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.key-value-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>
