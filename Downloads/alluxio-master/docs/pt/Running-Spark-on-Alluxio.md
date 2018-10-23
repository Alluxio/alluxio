---
layout: global
title: Rodando Spark no Alluxio
nickname: Apache Spark
group: Frameworks
priority: 0
---

* Table of Contents
{:toc}

Este guia descreve como rodar o [Apache Spark](http://spark-project.org/) no Alluxio e utilizar
o `HDFS` como o sistema de armazenamento inferior. Atente que o Alluxio suporta vários
`under storage systems` além do HDFS e permite que estruturas como o `Spark` leiam ou
escrevam dados nestes sistemas.

## Compatibilidade

O Alluxio funciona com o `Spark 1.1` ou superiores `out-of-the-box`.

## Pré-requisitos

* Alluxio `cluster` deve ser configurado de acordo com os guias
[Local Mode](Running-Alluxio-Locally.html) ou [Cluster Mode](Running-Alluxio-on-a-Cluster.html).

* Alluxio `client` irá precisar ser compilado com o perfil específico do `Spark`. Construa o
projeto inteiro a partir do diretório raiz do Alluxio com o comando:

```bash
$ mvn clean package -Pspark -DskipTests
```

* Adicione a linha seguinte para `spark/conf/spark-defaults.conf`:

```bash
spark.driver.extraClassPath {{site.ALLUXIO_CLIENT_JAR_PATH}}
spark.executor.extraClassPath {{site.ALLUXIO_CLIENT_JAR_PATH}}
```

* Se o Alluxio está rodando com o `Hadoop 1.x cluster`, crie um novo arquivo
`spark/conf/core-site.xml` com o conteúdo seguinte:

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
```

* Se você está rodando o Alluxio no modo de tolerância a falha com o `zookeper` e o
`Hadoop 1.x cluster`, adicione as entradas a seguinte no arquivo, criado anteriormente,
`spark/conf/core-site.xml`:

```xml
<property>
  <name>fs.alluxio-ft.impl</name>
  <value>alluxio.hadoop.FaultTolerantFileSystem</value>
</property>
```

E as linhas seguintes em `spark/conf/spark-defaults.conf`:

```bash
spark.driver.extraJavaOptions -Dalluxio.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181 -Dalluxio.zookeeper.enabled=true
spark.executor.extraJavaOptions -Dalluxio.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181 -Dalluxio.zookeeper.enabled=true
```

## Utilize o Alluxio como Entrada e Saída

Esta seção demonstra como usar o Alluxio como entrada e a saída para aplicações `Spark`.

Coloque o arquivo `LICENSE` dentro do `HDFS`, assumindo a que o `namenode` está rodando no `localhost`:

```bash
$ hadoop fs -put -f /alluxio/LICENSE hdfs://localhost:9000/alluxio/LICENSE
```

Execute os comandos seguintes pelo `spark-shell`, assumindo a que o Alluxio `Master` está rodando
no `localhost`:

```scala
> val s = sc.textFile("alluxio://localhost:19998/LICENSE")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/LICENSE2")
```

Abra o seu navegador e acesse [http://localhost:19999](http://localhost:19999). Deve haver um arquivo
de saída `LICENSE2` que duplica cada linha do arquivo `LICENSE`.

Quando o Alluxio estiver rodando no modo de tolerância a falha, você pode apontar para qualquer
Alluxio `master`:

```scala
> val s = sc.textFile("alluxio-ft://stanbyHost:19998/LICENSE")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio-ft://activeHost:19998/LICENSE2")
```

## Data Locality

Se a tarefa de `locality` do `Spark` está como `ANY` enquanto deveria ser `NODE_LOCAL`, é devido,
provavelmente, que o Alluxio e `Spark` usem versões diferentes de representações de endereço de rede,
talvez um destes utilize o nome do servidor enquanto que o outro utiliza o endereço de `IP`. Por
favor, atente à [este `ticket jira`](https://issues.apache.org/jira/browse/SPARK-10149) para maiores
detalhes, onde você pode encontrar soluções a partir da comunidade `Spark`.

Nota: O Alluxio utiliza o nome do servidor para representar o endereço de rede, com exceção da versão
0.7.1 onde o endereço `IP` é utilizado. O `Spark 1.5.x` possui o Alluxio 0.7.1 por padrão, e neste
caso, o padrão é que ambos o `Spark` e o Alluxio utilizem o endereço de `IP` para representar o endereço
da rede, então a localidade de dados deverá funcionar. But desde a versão 0.8.0, visando consistência
com o `HDFS`, o Alluxio representa o endereço de rede através do `hostname`. Existe uma solução de
contorno quando rodar o `Spark` para alcançar a localização de dados. Usuários podem especificar
explicitamente o nome do servidor através do `script` fornecido no `Spark`. Inicie um `Spark Worker`
em cada `slave node` com o `slave-hostname`:

```bash
$ $SPARK_HOME/sbin/start-slave.sh -h <slave-hostname> <spark master uri>
```

Por exemplo:

```bash
$ $SPARK_HOME/sbin/start-slave.sh -h simple30 spark://simple27:7077
```

Você também pode definir o `SPARK_LOCAL_HOSTNAME` em `$SPARK_HOME/conf/spark-env.sh` para obter sucesso
nisto. Por exemplo:

```properties
SPARK_LOCAL_HOSTNAME=simple30
```

De qualquer forma, os endereços do `Spark Worker` serão o `hostname` e o `Locality Level` será
`NODE_LOCAL`, como demonstrado no `Spark WebUI` abaixo:

![hostname]({{site.data.img.screenshot_datalocality_sparkwebui}})

![locality]({{site.data.img.screenshot_datalocality_tasklocality}})
