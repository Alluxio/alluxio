---
layout: global
title: FileSystem Client API
nickname: FileSystem API
group: Features
priority: 1
---

* Table of Contents
{:toc}

Alluxio fornece um acesso ao dado através de uma interface de `FileSystem`. Os arquivos no Alluxio 
oferecem uma lógica de escrita única: estes tornam-se imutáveis depois que forem escritos por 
completo e não podem ser lidos enquanto não estiverem completos. Alluxio fornece duas diferentes 
`FileSystem APIs`, uma nativa `API` e outra compatível com a `Hadoop API`. A nativa oferece 
melhor performance enquanto que a possui compatibilidade com o `Hadoop` concede aos usuários uma 
maior flexibilidade ao Alluxio pois não requer modificação do código existente que utiliza a 
`Hadoop API`.

# API Nativa

O Alluxio fornece um `API` estilo `Java` para acessar e modificar os arquivos dentro do Alluxio. 
Todos os recursos são especificados através de uma `AlluxioURI` que representa o caminho para o
recurso.

### Obtendo um File System Client

Para obter um Alluxio `client file system` em código `Java`, execute:

{% include File-System-API/get-fileSystem.md %}

### Criando um Arquivo

Todas as operações de metadados assim como a abertura de um arquivo para leitura ou criação de um 
arquivo para escrita, são operações executadas através do objeto `FileSystem`. Já que os arquivos 
no Alluxio são imutáveis depois de escritos, a forma idiomática para criar arquivos é com a 
utilização de `FileSystem#createFile(AlluxioURI)`, que retorna um objeto `stream` e este pode ser 
utilizado para escrever o arquivo. Por exemplo:

{% include File-System-API/write-file.md %}

### Especificando Opções de Operações

Para todas as operações de `FileSystem`, campos de opções adicionais podem ser especificados, 
que permitem usuários mencionar configurações fora do padrão para a operação. Por exemplo:

{% include File-System-API/specify-options.md %}

### Opções de I/O

Os usuários do Aluuxio utilizam dois tipos diferentes de armazenamento: o armazenamento gerenciado 
pelo Alluxio e o armazenamento inferior (`under storage`). O armazenamento do Alluxio é a memória, 
`SSD` e/ou o `HDD` alocado para os Alluxio `workers`. O `Under Storage` é o recurso de armazenamento 
gerenciado pelo sistema da camada inferior de armazenamento, como um: `S3`; `Swift` ou `HDFS`. Os 
usuários podem especificar a interação do armazenamento do Alluxio com o armazenamento inferior 
através do `ReadType` e do `WriteType`. O `ReadType` especifica o comportamento da leitura do dado 
quando estiver lendo um novo arquivo, exemplo, quando o dado deve ser guardado no armazenamento do 
Alluxio. E o `WriteType` especifica o comportamento de escrita do dado quando um arquivo é escrito, 
exemplo, se um dado for escrito no armazenamento do Alluxio.

Segue abaixo uma tabela com os comportamentos esperados do `ReadType`. As leituras irão sempre tomar 
preferência no armazenamento do Alluxio do que no `under storage system`.

<table class="table table-striped">
<tr><th>Read Type</th><th>Behavior</th>
</tr>
{% for readtype in site.data.table.ReadType %}
<tr>
  <td>{{readtype.readtype}}</td>
  <td>{{site.data.table.en.ReadType.[readtype.readtype]}}</td>
</tr>
{% endfor %}
</table>

Segue abaixo uma tabela com os comportamentos esperados do `WriteType`.

<table class="table table-striped">
<tr><th>Write Type</th><th>Behavior</th>
</tr>
{% for writetype in site.data.table.WriteType %}
<tr>
  <td>{{writetype.writetype}}</td>
  <td>{{site.data.table.en.WriteType.[writetype.writetype]}}</td>
</tr>
{% endfor %}
</table>

### Política de Localização

O Alluxio fornece uma política de localização para onde os `workers` devam armazenar os blocos de 
um arquivo. O usuário pode definir a política em `CreateFileOptions` para escrita de arquivos e em 
`OpenFileOptions` para a leitura de arquivos no Alluxio. O Alluxio suporta uma política customizada 
e as políticas embutidas incluídas são:

Alluxio provides location policy to choose which workers to store the blocks of a file. User can set
the policy in `CreateFileOptions` for writing files and `OpenFileOptions` for reading files into
Alluxio. Alluxio supports custom location policy, and the built-in polices include:

* **LocalFirstPolicy**

	Retorna o primeiro servidor local e se o `worker` local não possuir capacidade suficiente para
	armazenar um bloco, será escolhido um `worker` aleatoriamente a partir da lista ativa de 
	`workers`. Esta é a política padrão.

* **MostAvailableFirstPolicy**

    Retorna o `worker` com maior quantidade de `bytes` disponíveis.

* **RoundRobinPolicy**

	A escolha pelo `worker` para o próximo bloco ocorre como `round-robin` e os `workers` que não 
	possuem capacidade são omitidos.

* **SpecificHostPolicy**

	Retorna um `worker` com o nome do servidor específico. Esta política não pode ser definida 
	como padrão.

O Alluxio suporta políticas customizadas, você pode também desenvolver a sua própria política 
apropriada para a sua carga de trabalho. Atente que a política padrão deve ter um construtor 
vazio e para utilizar a escrita do tipo `ASYNC_THROUGH`, todos os blocos de um arquivo devem estar
no mesmo `worker`.

### Acessando um Arquivo Existente no Alluxio

Todas as operações em arquivos ou diretórios existentes exigem que o usuário especifique o 
`AlluxioURI`. Com o `AlluxioURI`, o usuário pode utilizar qualquer um dos métodos do `FileSystem` 
para acessar o recurso.

### Lendo os Dados

Um `AlluxioURI` pode ser utilizado para efetuar operações de `FileSystem` do Alluxio, como 
modificar os metadados de um arquivo, exemplos: `TTL`; fixar o arquivo (`pin`); ou pegar uma
leitura de `stream` para ler um arquivo.

Por exemplo, para ler um arquivo:

{% include File-System-API/read-file.md %}

# Hadoop API

O Alluxio possui um empacotador do `client` nativo que fornece compatibilidade coma interface 
`FileSystem`. Com este `client`, as operações de arquivo do `Hadoop` serão traduzidas para as 
operações de `FileSystem`. A última documentação da interface `FileSystem` pode ser encontrada
[aqui](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html).

A interface compatível do `Hadoop` é fornecida com uma classe de conveniência, permitindo que 
os usuários mantenham o código anterior escrito para o `Hadoop`.
