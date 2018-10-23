---
layout: global
title: Alluxio Standalone com Tolerância a Falha
nickname: Alluxio Standalone com Tolerância a Falha
group: Deploying Alluxio
priority: 3
---

* Table of Contents
{:toc}

Tolerância a Falha no Alluxio é baseado em uma abordagem `multi-master` onde múltiplos processos `master`
estão em execução. Um destes processos é eleito o líder e é utilizado por todos os outros `workers` e
`clients` como primeiro ponto de contato. Os outros `masters` atuam como `standbys` utilizando um
registro compartilhado para garantir que eles sustentem o mesmo metadados dos `file systems` como um
novo líder e possam assumir, rapidamente, em caso de falha de um líder `master`.

Se um líder falhar, o novo líder é automaticamente eleito a partir dos `masters` em espera que estão
disponíveis e o Alluxio segue a atividade normal. Atente que enquanto ocorre a transferência para um
`standby master`, alguns `clients` podem perceber um breve atraso ou até falhas curtas.

## Pré-requisito

Existem dois pré-requisitos para configuração de um `Alluxio Cluster` com tolerância a falha:

* [ZooKeeper](http://zookeeper.apache.org/)
* Um sólido armazenamento inferior sólido para onde deve ser armazenados o registro (`journal`).

Alluxio necessita do `ZooKeeper` para tolerância a falha devido a seleção do líder. Isto garante que
haverá apenas um líder `master` para o Alluxio em um determinado tempo.

Alluxio também necessita de um `under storage system` compartilhado para onde irá armazenar o `journal`.
Este `file system` compartilhado deverá estar acessível por todos os `masters`, as possíveis opções são
[HDFS](Configuring-Alluxio-with-HDFS.html), [Amazon S3](Configuring-Alluxio-with-S3.html) ou
[GlusterFS](Configuring-Alluxio-with-GlusterFS.html). O líder `master` escreve o registro para o
`file system` compartilhado, enquanto os outros `standby masters` mantém atualizados a partir da
repetição continua do `journal`.

### ZooKeeper

Conforme mencionado anteriormente, Alluxio utiliza o `ZooKeeper` para alcançar a tolerância a falha.
Os Alluxio `masters` utilizam o `ZooKeper` para eleição do líder. Os Alluxio `clients` também utilizam
o `ZooKeeper` para pesquisar a identidade e endereço do líder atual.

`ZooKerper` deve ser configurado de formar independente
(analise o [ZooKeeper Getting Started](http://zookeeper.apache.org/doc/r3.4.5/zookeeperStarted.html)).

Depois que o `ZooKeeper` estiver configurado, guarde o endereço e porta para configuração do Alluxio
abaixo.

### File System Compartilhado para Registro (Journal)

Allxuio requer um `file system` compartilhado para armazenamento do `journal`. Todos os `masters devem
conseguir ler e escrever neste compartilhamento. Apenas o líder `master` irá escrever no registro em
um determinado tempo, porém todos os `masters` leem o `journal` compartilhado para atualizar o estado
do Alluxio.

Este `file system` compartilhado deve ser configurado independente do Alluxio e deve estar em execução
antes que o Alluxio inicialize.

Por exemplo, se estiver utilizando `HDFS` para o registro compartilhado, você deverá possuir o endereço
e porta do `NameNode` que está em execução, pois você precisará para configurar o Alluxio abaixo.

## Configurando o Alluxio

Assim que o `ZooKeeper` e o `file system` compartilhado estiverem rodando, você precisará configurar o
arquivo `alluxio-env.sh` apropriadamente em cada servidor.

### Endereço Externo Visivelmente

Nas seções seguintes, referenciamos um "endereço externo visivelmente". Isto é, simplesmente, o endereço
de uma interface da máquina que está sendo capturada que pode ser vista por outros nós do `Alluxio Cluster`.
No `EC2`, você de utilizar o endereço `ip-x-x-x-x`. Em particular, não utiliza `localhost` ou `127.0.0.1`,
já que os outros nós não poderão acessar o seu nó.

### Configurando o Alluxio com Tolerância a Falha


Para habilitar a tolerância a falha para o Alluxio, deverá ser efetuada configurações adicionais para os
Alluxio `masters`, `workers` e `clients`. Em `conf/alluxio-env.sh`, as opções `java` devem ser definidas:

<table class="table">
<tr><th>Property Name</th><th>Value</th><th>Meaning</th></tr>
{% for item in site.data.table.java-options-for-fault-tolerance %}
<tr>
  <td>{{item.PropertyName}}</td>
  <td>{{item.Value}}</td>
  <td>{{site.data.table.en.java-options-for-fault-tolerance[item.PropertyName]}}</td>
</tr>
{% endfor %}
</table>

Para definir estas opções, você deve configurar `ALLUXIO_JAVA_OPTS` para incluir:

    -Dalluxio.zookeeper.enabled=true
    -Dalluxio.zookeeper.address=[zookeeper_hostname]:2181

Se você estiver utilizando um `cluster` de nós do `ZooKeeper`, você pode especificar endereços múltiplos,
separando-os com vírgulas, como:

    -Dalluxio.zookeeper.address=[zookeeper_hostname1]:2181,[zookeeper_hostname2]:2181,[zookeeper_hostname3]:2181

Alternativamente, estas configurações devem ser definidas no arquivo `alluxio-site.properties`. Maiores
detalhes sobre os parâmetros podem ser encontrados em [Definições de Configuração](Configuration-Settings.html).

### Configuração do Master

Além das configurações acima, Alluxio `masters` precisam de configurações adicionais. A variável a seguir
deve ser definida corretamente em `conf/alluxio-env.sh`:

    export ALLUXIO_MASTER_HOSTNAME=[endereço externamente visível desta máquina]

Também deve ser informado o diretório correto do `journal` através do parâmetro `alluxio.master.journal.folder`
para `ALLUXIO_JAVA_OPTS`. Por exemplo, se você está utilizando `HDFS` para o `journal`, você pode adicionar:

    -Dalluxio.master.journal.folder=hdfs://[ServidorDoNamenode]:[PortaDoNamenode]/caminho/para/alluxio/journal

Assim que todos os Alluxio `masters` estiverem configurados desta forma, estes podem ser iniciados para
tolerância a falha. Um dos `masters` se tornará o líder e os outros iniciarão a repetição do `journal` e
esperarão até o `master` atual falhe.

### Configuração do Worker

Enquanto os parametros de configuração acima estiverem definidos corretamente, o `worker` estará apto para
consultar com o `ZooKeeper` e encontrar o líder `master` atual para conexão. Sendo assim, a variável
`ALLUXIO_MASTER_HOSTNAME` não deve ser configurada para os `workers`.

### Configuração do Client

Nenhuma configuração adicional são solicitadas pelo modo de tolerância a falha para os `clients`. Enquanto ambas:

    -Dalluxio.zookeeper.enabled=true
    -Dalluxio.zookeeper.address=[zookeeper_hostname]:2181

estiverem definidas corretamente para a sua aplicação `client`, a aplicação irá se consultar o líder `mestre` atual
pelo `ZooKeeper`.
