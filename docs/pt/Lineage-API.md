---
layout: global
title: Lineage Client API (alpha)
nickname: Lineage API
group: Features
priority: 2
---

* Table of Contents
{:toc}

O Alluxio pode alcançar alta vazão de escrita e leitura, sem comprometer a tolerância a falhar 
através de *Lineage* (Linhagem), onde resultados perdidos são recuperados através da re-execução 
de rotinas que criaram o resultado.

Com o `lineage`, os resultados das aplicações são escritos em memória, e o `checkpoints` dos 
resultados ocorrem periodicamente no `under storage system`, de forma assíncrona. No caso de falhas, 
o Alluxio executar uma *rotina de re-processamento* para restaurar os arquivos perdidos. O `lianeage` 
assume que as rotinas são determinista então os resultados do reprocessamento são idênticos. Se 
esta suposição não for comprida, fica a critério da aplicação de resolver os resultados divergentes.

# Hablitando Lineage

Por padrão, o `linaeage` não é habilitado. Este pode ser habilitado definindo o valor da propriedade  
`alluxio.user.lineage.enabled` para `true` no arquivo de configuração.

# Lineage API

O Alluxio provê uma `API` estilo `Java` para gerenciamento e acesso das informações de linhagem.

### Obtendo um Lineage Client

Para obter um Alluxio `Lineage Client` em código `Java`, execute:

{% include Lineage-API/get-lineage-client.md %}

### Criando o Registro de Lineage

O `Lineage` pode ser criado através da execução de 
`AlluxioLineage#createLineage(List<AlluxioURI>, List<AlluxioURI>, Job)`. O registro do `lineage` 
obtém (1) uma lista de `URIs` de arquivos de entrada, (2) uma lista de de `URIs` arquivos de saída e 
(3) uma tarefa. A tarefa é a descrição de um program que pode ser executado pelo Alluxio para 
reprocessar os arquivos resultantes a partir de arquivos de entrada. *Nota: Na versão `alpha` atual, 
somente o comando `CommandLineJob` é suportado, qual simplesmente obtém um comando `String` que 
pode ser executado em um terminal. O usuário precisa prover as configurações necessárias e variáveis 
ambientes de execução para garantir que o comando pode ser executado ed ambos o `client` e o `master`
(durante o reprocessamento).*

Por exemplo,
{% include Lineage-API/config-lineage.md %}

A função `createLineage` retorna um `id` de um recém registro de linhagem criado. Antes de 
criar este registro, tenha certeza que todos os arquivos de entrada estão mantidos ou especifique 
um arquivo de saída de outro registro de `lineage`.

### Especificando Opções de Operações

Para todas as operações do `AlluxioLineage`, campos de opções adicionais devem ser preenchidos 
para que permitam os usuários de especificar definições que não são padrões para a operação.

### Apagando um Lineage

Um registro de `lineage` pode ser apagado através do comando `AlluxioLineage#deleteLineage`. 
Deve ser informando um `id` para esta função.

{% include Lineage-API/delete-lineage.md %}

Por padrão, um registro de `lineage` a ser apagado não pode possuir arquivos resultantes 
dependentes em outros `lineages`. Opcionalmente, todo o conjunto de `lineages` pode ser apagado 
de uma única vez, utilizando a opção em cascata. Por exemplo:

{% include Lineage-API/delete-cascade.md %}

# Parâmetros de Configuração para o Lineage

Estes são os parâmetros de configuração relacionados as funcionalidades do `lineage`.

<table class="table table-striped">
<tr><th>Parameter</th><th>Default Value</th><th>Description</th></tr>
</tr>
{% for record in site.data.table.LineageParameter %}
<tr>
  <td>{{record.parameter}}</td>
  <td>{{record.defaultvalue}}</td>
  <td>{{site.data.table.en.LineageParameter.[record.parameter]}}</td>
</tr>
{% endfor %}
</table>
