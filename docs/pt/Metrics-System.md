---
layout: global
title: Métricas do Sistema
group: Features
priority: 3
---

* Table of Contents
{:toc}

As métricas provem uma idéia do que está acontecendo com o `cluster`. Estas são um recurso inestimável
para monitoração e depuração. Alluxio possui um sistema configurável de métricas baseado no  
[Coda Hale Metrics Library](https://github.com/dropwizard/metrics). No sistema de métricas, as métricas
origens são as métricas geradas e as métricas filtros consomem os registros gerados pelas métricas origens.
O sistema de métricas elege periodicamente métricas origens e passa seus registros para as métricas filtros.

As métricas do Alluxio são particionadas em diferentes instâncias correspondendo aos componentes do Alluxio.
Dentre cada instância, os usuários podem configurar um conjunto de filtros para quais as métricas serão
relatar. As seguintes instâncias estão suportadas atualmente:

* Master: O processo master independente do Alluxio.
* Worker: O processo worker independente do Alluxio.

Cada instância pode relatar entre zero ou mais filtros.

* ConsoleSink: Produz os valores de métricas para o console.
* CsvSink: Exporta os dados das métricas para arquivos CSV em intervalos regulares.
* JmxSink: Registra métricas para visualicação em um console JMX.
* GraphiteSink: Envia métricas para um servidor Graphite.
* MetricsServlet: Adiciona um servlet dentro da Web UI para disponibilizar as métricas como dados JSON.

Algumas métricas como `BytesReadLocal` dependem de dados coletados a partir de um `client heartbeat`.
Para pegar uma métrica preciso, é esperado que o `client` feche o `AlluxioFileSystem client`depois de
utiliza-lo.

## Configuração
O sistema de métricas é configurado através de um arquivo de configuração que o Alluxio espera estar
presente em `$ALLUXIO_HOME/conf/metrics.properties`. Um local personalizado pode ser especificado na
propriedade de configuração `alluxio.metrics.conf.file`. Alluxio prove um arquivo
(metrics.properties.template) dentro do diretório `conf` que possui todos as propriedades de
configuração. Por padrão, `MetricsServlet` é habilitado e você pode enviar requisições `HTTP`
"/metrics/json" para pegar uma imagem de todas as métricas registradas em formato JSON.

## Métricas Suportadas

As métricas são classificadas como:

* Geral: medições gerais do `cluster` (exemplo CapacityTotal).
* Operações Lógicas: número de operações realizadas (exemplo FilesCreated).
* Invocações RPC: número de invocações RPC por operação (exemplo CreateFileOps).

A seguir são informados os detalhes das métricas disponíveis.

### Master

#### Geral

* CapacityTotal: Capacidade total de `file system` em `bytes`.
* CapacityUsed: Capacidade utilizada de `file system` em `bytes`.
* CapacityFree: Capacidade disponível de `file system` em `bytes`.
* PathsTotal: Número total de arquivos e diretórios dentro de um `file system`.
* UfsCapacityTotal: Capacidade total do `under file system` em `bytes`.
* UfsCapacityUsed: Capacidade utilizada do `under file system` em `bytes`.
* UfsCapacityFree: Capacidade disponível do `under file system` em `bytes`.
* Workers: Número de `workers`.

#### Operações Lógicas

* DirectoriesCreated: Número total de diretórios criados.
* FileBlockInfosGot: Número total da informação de blocos de arquivos retornados.
* FileInfosGot: Número total da informação de arquivos retornados.
* FilesCompleted: Número total de arquivos completos.
* FilesCreated: Número total de arquivos criados.
* FilesFreed: Número total de arquivos liberados.
* FilesPersisted: Número total de arquivos mantidos.
* FilesPinned: Número total de arquivos fixados.
* NewBlocksGot: Número total de novos blocos recebidos.
* PathsDeleted: Número total de arquivos e diretórios apagados.
* PathsMounted: Número total de diretórios montados.
* PathsRenamed: Número total de arquivos e diretórios renomeados.
* PathsUnmounted: Número total de diretórios desmontados.

#### Invocações RPC

* CompleteFileOps: Número total de operações `CompleteFile`.
* CreateDirectoryOps: Número total de operações `CreateDirectory`.
* CreateFileOps: Número total de operações `CreateFile`.
* DeletePathOps: Número total de operações `DeletePath`.
* FreeFileOps: Número total de operações `FreeFile`.
* GetFileBlockInfoOps: Número total de operações `GetFileBlockInfo`.
* GetFileInfoOps: Número total de operações `GetFileInfo`.
* GetNewBlockOps: Número total de operações `GetNewBlock`.
* MountOps: Número total de operações `Mount`.
* RenamePathOps: Número total de operações `RenamePath`.
* SetAttributeOps: Número total de operações `SetAttribute`.
* UnmountOps: Número total de operações `Unmount`.

### Worker

#### Geral

* CapacityTotal: Capacidade total do `worker` em `bytes`.
* CapacityUsed: Capacidade utilizada do `worker` em `bytes`.
* CapacityFree: Capacidade dispon[ivel do `worker` em `bytes`.

#### Operações Lógicas

* BlocksAccessed: Número total de blocos acessados.
* BlocksCached: Número total de blocos em `cache`.
* BlocksCanceled: Número total de blocos cancelados.
* BlocksDeleted: Número total de blocos deletados.
* BlocksEvicted: Número total de blocos expulsos.
* BlocksPromoted: Número total de blocos promovidos.
* BlocksReadLocal: Número total de blocos lidos localmente pelo `worker`.
* BlocksReadRemote: Número total de blocos lidos remotamente pelo `worker`.
* BlocksWrittenLocal: Número total de blocos escritos localmente para o `worker`.
* BytesReadLocal: Número total de `bytes` lidos localmente pelo `worker`.
* BytesReadRemote: Número total de `bytes` lidos remotamente pelo `worker`.
* BytesReadUfs: Número total de `bytes` lidos pelo `under file system` no `worker`.
* BytesWrittenLocal: Número total de `bytes` escritos localmente para o `worker`.
* BytesWrittenUfs: Número total de `bytes` escritos para o `under file system` no `worker`.
