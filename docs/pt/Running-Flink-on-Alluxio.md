---
layout: global
title: Rodando Apache Flink no Alluxio
nickname: Apache Flink
group: Frameworks
priority: 2
---

Este guia descreve como rodar o Alluxio com [Apache Flink](http://flink.apache.org/) para
que você trabalhe com os arquivos armazenados no Alluxio.

# Pré-requisitos

O pré-requisito para esta parte é que você possua [Java](Java-Setup.html). Nós também 
consideramos que você tenha configurado o Alluxio de acordo com estes guias [Local Mode](Running-Alluxio-Locally.html) 
ou [Cluster Mode](Running-Alluxio-on-a-Cluster.html).

Por favor, siga os guias para configuração do `Flink` no `Apache Flink` [website](http://flink.apache.org/).

# Configuração

O `Apache Flink` permite utilizar o Alluxio através de um `generic file system wrapper` para o 
`Hadoop file system`. Sendo assim, a configuração do Alluxio é alcançada devido aos arquivos 
de configuração do `Hadoop`.

#### Configurar a propriedade em `core-site.xml`

Se você possui uma configuração `Hadoop` com uma instalação `Flink`, adicione a seguinte 
propriedade no arquivo de configuração `core-site.xml`:

{% include Running-Flink-on-Alluxio/core-site-configuration.md %}

Caso você não possua uma configuração `Hadoop`, você tem que criar um arquivo chamado `core-site.xml` com 
o conteúdo a seguir:

{% include Running-Flink-on-Alluxio/create-core-site.md %}

#### Especifique o caminho para `core-site.xml` em `conf/flink-config.yaml`

Em seguida, você deve especificar o caminho para a configuração do `Hadoop` no `Flink`. Abra 
o arquivo `conf/flink-config.yaml` no diretório raiz do `Flink` e defina o valor de configuração 
`fs.hdfs.hadoopconf` para o **diretório** que consta no `core-site.xml` (Para versões mais 
recentes do `Hadoop`, normalmente, este diretório termina com `etc/hadoop`).

#### Tornar o Alluxio Client jar disponível para o Flink

No último passo, nós precisamos tornar disponível o arquivo `jar` do Alluxio para o `Flink`, porque 
este contém a classe configurada `alluxio.hadoop.FileSystem`.

Existem diferentes maneiras de efetuar isto:

- Coloque o arquivo `alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar` 
dentro do diretório `lib` do `Flink` (para configurações locais ou em `cluster`)
- Coloque o arquivo `alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`
dentro do diretório `ship` para o `Flink` no `YARN`.
- Especifique o local do arquivo `jar` na variável de ambiente `HADOOP_CLASSPATH` (tenha certeza que 
isto estará disponível em todo o cluster). Por exemplo:

{% include Running-Flink-on-Alluxio/hadoop-classpath.md %}

# Utilizando o Alluxio com Flink

Para utilizar o Alluxio com `Flink`, apenas especifique o caminho com o `scheme alluxio://`.

Se o Alluxio está instalado localmente, o um caminho válido deve ser algo como: 
`alluxio://localhost:19998/user/hduser/gutenberg`.
