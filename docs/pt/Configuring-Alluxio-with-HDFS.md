---
layout: global
title: Configurando o Alluxio com HDFS
nickname: Alluxio com HDFS
group: Under Store
priority: 3
---

* Table of Contents
{:toc}

Este guia descreve como configurar o Alluxio com
[HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
como um sistema de armazenamento inferior.

## Configuração Inicial

Para rodar um Allucio `cluster` em um conjunto de máquinas, você deve colocar os binários do
Alluxio em cada uma destas máquinas. Você pode
[compilar o Alluxio](Building-Alluxio-Master-Branch.html) ou
[baixar os binários localmente](Running-Alluxio-Locally.html).

Atente que, por padrão, os binários do Alluxio foram construídos para trabalhar com a versão
`Hadoop HDFS 2.2.0`. Para usar outra versão, você precisa recompilar os binários a partir do
código fone com a versão correta do `Hadoop`, que será definido nos próximos passos. Para isso,
supomos que o diretório raiz do código fonte do Alluxio é `${ALLUXIO_HOME}`.

* Modifique o rótulo `hadoop.version` em `${ALLUXIO_HOME}/pom.xml`. Exemplo, para trabalhar com
o `Hadoop 2.6.0`, modifique arquivo `pom` para definir o rótulo como  
"`<hadoop.version>2.6.0</hadoop.version>`" ao invés de
"`<hadoop.version>2.2.0</hadoop.version>`". Depois, recompile o código usando o `maven`.

{% include Configuring-Alluxio-with-HDFS/mvn-package.md %}

* Alternativamente, você também pode passar a versão correta do `Hadoop` na linha de comando quando
for compilar utilizando o `maven`. Por exemplo, se você quer que o Alluxio trabalher com a versão
`Hadoop HDFS 2.6.0`:

{% include Configuring-Alluxio-with-HDFS/mvn-Dhadoop-package.md %}

Se for obtido sucesso, você deve ver o arquivo
`alluxio-assembly-server-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar` criado dentro do
diretório `assembly/server/target` e este é o arquivo `jar` que você pode utilizar para executar os
Alluxio `Master` e `Worker`.

## Configurando o Alluxio

Para rodar o binário do Alluxio, devemos definir os arquivos de configuração. Crie o seu arquivo
de configuração a partir do modelo:

{% include Common-Commands/copy-alluxio-env.md %}

Então, altere o arquivo `alluxio-env.sh` para definir o endereço do `under storage` para o
endereço do `HDFS Namenode` (exemplo, `hdfs://localhost:9000` se você estiver executando um
`HDFS Namenode` localmente com a porta padrão).

{% include Configuring-Alluxio-with-HDFS/underfs-address.md %}

## Rodando o Alluxio Localmente com HDFS

Depois que tudo estiver configurado, você pode iniciar o Alluxio localmente para ver se tudo
funciona.

{% include Common-Commands/start-alluxio.md %}

Isso deve iniciar um Alluxio `master` e um Alluxio `worker` localmente. Você pode ver a
interface de usuário do `master` em [http://localhost:19999](http://localhost:19999).

Em seguida, você pode executar um simples programa de teste:

{% include Common-Commands/runTests.md %}

Depois que obter sucesso neste teste, você pode visitar a interface de usuário `web` do
`HDFS` em [http://localhost:50070](http://localhost:50070), para verificar se os arquivos
criados pelo Alluxio existem. Para este teste, você deve ver arquivos nomeados como:
`/alluxio/data/default_tests_files/BasicFile_STORE_SYNC_PERSIST`

Você pode parar o Alluxio a qualquer tempo, executando:

{% include Common-Commands/stop-alluxio.md %}
