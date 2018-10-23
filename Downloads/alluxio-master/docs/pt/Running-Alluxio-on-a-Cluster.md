---
layout: global
title: Alluxio Standalone em um Cluster
nickname: Alluxio Standalone em um Cluster
group: Deploying Alluxio
priority: 2
---

* Table of Contents
{:toc}

## Standalone em um Cluster

Primeiro, baixe o arquivo `tar` do Alluxio e o extraia.

{% include Running-Alluxio-on-a-Cluster/download-extract-Alluxio-tar.md %}

No diretório `alluxio/conf`, copie `alluxio-env.sh.template` para `alluxio-env.sh`. Tenha certeza
que `JAVA_HOME` aponta para uma instalação válida do `Java 7`. Atualize o `ALLUXIO_MASTER_HOSTNAME`
para o `hostname` da máquina que você deseja ser o Alluxio `Master`. Adicione o endereço de `IP` de
todos os `Workers Nodes` no arquivo `alluxio/conf/workers`. Finalmente, sincronize todas as
informações para os servidores `workers`. Você pode executar

{% include Running-Alluxio-on-a-Cluster/sync-info.md %}

para sincronizar os arquivos e diretórios de todos os servidores especificados no arquivo
`alluxio/conf/workers`.

Agora, você iniciar o Alluxio:

{% include Running-Alluxio-on-a-Cluster/start-Alluxio.md %}

Para verificar se o Alluxio está em execução, visite o endereço `http://<alluxio_master_hostname>:19999`,
também cheque o diretório de log `alluxio/logs` ou execute um programa de teste:

{% include Running-Alluxio-on-a-Cluster/run-tests.md %}

**Nota**: Se você estiver utilizando o `EC2`, tenha certeza que as definições de segurança de grupo
no servidor `master` permite conexões de entrada na porta `web UI` do Alluxio.

## Utilizando o argumento bootstrapConf para o script bin/alluxio

O `script` do Alluxio também contém uma lógica para criar uma configuração básica para um `cluster`.
Se você executar:

{% include Running-Alluxio-on-a-Cluster/bootstrapConf.md %}

e não possuir nenhum arquivo `alluxio/conf/alluxio-env.sh`, então o `script` irá criar um arquivo com
as definições apropriadas para um `cluster` com um nó `master` executando no `<alluxio_master_hostname>`.

O `script` precisa ser executado em todos os nós que você deseja configurar.

O `script` irá configurar seus `workers` para usar 2/3 da memória total de cada `worker`. Esta quantidade
pode ser modificar através da alteração do arquivo `alluxio/conf/alluxio-env.sh` no `worker`.

## EC2 Cluster com Spark

Se você usa `Spark` em um `EC2 Cluster`, o Alluxio será instalado e configurado por padrão.
