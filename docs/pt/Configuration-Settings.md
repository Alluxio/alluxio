---
layout: global
title: Definições de Configuração
group: Features
priority: 1
---

* Table of Contents
{:toc}

Existem dois tipos de parâmetros de configuração para o Alluxio:

1. [Propriedades de Configuração](#propriedades-de-configuração) são utilizadas para configurar 
definições de tempo de execução do sistema do Alluxio e
2. [Propriedades de ambiente do sistema](#propriedades-ambiente-do-sistema) controlam as opções do `Java VM` 
para rodar o Alluxio assim como algumas definições básicas.

# Propriedades de Configuração

Na inicialização, Alluxio carrega as propriedades de configuração de um arquivo padrão (e opcionalmente
as configuração específicas).

1. Os valores padrões das propriedades de configuração do Alluxio são definidos em 
`alluxio-default.properties`. Este arquivo pode ser localizado no código fonte do Alluxio e é tipicamente 
distribuído com os binários do Alluxio. Nós não recomendamos que usuários iniciantes alterem este
arquivo diretamente.

2. Cada implantação de local e aplicação `client` também pode sobrescrever os valores padrões de 
propriedades no arquivo `alluxio-site.properties`. Atente que este arquivo **deve estar definido no
CLASSPATH** do `Java VM` em que o Alluxio estiver rodando. A maneira mais simples é de definir as 
configurações de arquivo local no diretório `$ALLUXIO_HOME/conf`.

Todas as propriedades de configuração do Alluxio se enquadram em seis categorias:
[Comum](#configurações-comum) (compartilhado pelo `Master` e `Worker`),
[Específica do Master](#configurações-do-master), [Específica do Worker](#configurações-do-worker),
[Específica do Usuário](#configurações-do-usuário), [Específico do Cluster](#gerenciamento-do-cluster) (utilizado
para rodar o Alluxio com gerenciados de `cluster` como `Mesos` e `YARN`) e
[Específica de Segurança](#configurações-de-segurança) (compartilhado pelo Master, Worker e Usuário).

## Configurações Comum

As configurações comum contém constantes compartilhadas por componentes diferentes.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.common-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.common-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Configurações do Master

A configuração do `master` especifica informações referentes ao nó `master`, como o endereço e número da porta.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.master-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.master-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Configurações do Worker

A configuração do `worker` especifica informações referentes ao nó `woeker`, como o endereço e número da porta.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.worker-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.worker-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>


## Configurações do Usuário

A configuração do `worker` especifica informações referentes ao acesso do `file system`.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.user-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.user-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Gerenciamento do Cluster

Quando o Alluxio roda com gerenciadores de `cluster` como `Mesos` e `YARN`, o Alluxio possui
opções de configuração adicionais.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.cluster-management %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.cluster-management.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Configurações de Segurança

As configurações de segurança especificam informações relacionadas as funcionalidades de segurança,
como autenticação e permissão de arquivo. As propriedades para autenticação tem efeito para o `master`,
`worker` e usuário. As propriedades de permissão de arquivo somente tem efeito para o `master`. Analise
a página [Segurança](Security.html) para maiores informações sobre funcionalidades de segurança.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.security-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.en.security-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Configure multihomed networks

Alluxio configuration provides a way to take advantage of multi-homed networks. If you have more
than one NICs and you want your Alluxio master to listen on all NICs, you can specify
`alluxio.master.bind.host` to be `0.0.0.0`. As a result, Alluxio clients can reach the master node
from connecting to any of its NIC. This is also the same case for other properties suffixed with
`bind.host`.

# Propriedades Ambiente do Sistema

Para executar o Alluxio, também é requerido que algumas variáveis de ambiente do sistema estejam
definidas corretamente, por padrão, estas são configuradas no arquivo `conf/alluxio-env.sh`. Se
este arquivo não existir ainda, você pode criar um a partir do modelo que nós disponibilizamos no
código fonte utilizando.

{% include Common-Commands/copy-alluxio-env.md %}

Existem algumas propriedades de configuração frequentemente utilizadas no Alluxio que podem ser
definidas através de variáveis de ambiente. Estas podem ser definidas através do terminal ou 
modificadas dos valores padrões especificados em `conf/alluxio-env.sh`.

* `$ALLUXIO_MASTER_ADDRESS`: endereço do Alluxio `master`, o padrão é o servidor local.
* `$ALLUXIO_UNDERFS_ADDRESS`: endereço do `under storage system`, o padrão é 
`${ALLUXIO_HOME}/underFSStorage` que é o `file system` local.
* `$ALLUXIO_JAVA_OPTS`: opções de `Java VM` para ambos `Master` e `Worker`.
* `$ALLUXIO_MASTER_JAVA_OPTS`: opções adicionais de `Java VM` para configuração do `Master`.
* `$ALLUXIO_WORKER_JAVA_OPTS`: opções adicionais de `Java VM` para configuração do `Worker`. Atente que,
por padrão, o `ALLUXIO_JAVA_OPTS` está incluído em ambos os `ALLUXIO_MASTER_JAVA_OPTS` e 
`ALLUXIO_WORKER_JAVA_OPTS`.

Por exemplo, se você quiser conectar o Alluxio no `HDFS` executando no servidor local e 
habilitar a depuração remota do `Java` na porta 7001, você pode utilizar:

{% include Configuration-Settings/more-conf.md %}
