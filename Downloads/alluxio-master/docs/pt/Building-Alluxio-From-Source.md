---
layout: global
title: Construindo o Alluxio Master Branch
nickname: Construindo o Master Branch
group: Resources
---

* Table of Contents
{:toc}

Este guia descreve os passos para compilar o Alluxio do início.

O pré-requisito para este guia é que você tenha [Java 7 (or above)](Java-Setup.html),
[Maven](Maven.html) e [Thrift 0.9.2](Thrift.html) (Optional) instalados.

Confira o Alluxio `master branch` a partir do Github e empacote-o:

```bash
$ git clone git://github.com/alluxio/alluxio.git
$ cd alluxio
$ mvn install -DskipTests
```

Se você estiver vendo o `java.lang.OutOfMemoryError: Java heap space`, por favor execute:

```bash
$ export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
```

Se você quer construir alguma versão particular do Alluxio, por exemplo {{site.ALLUXIO_RELEASED_VERSION}},
por favor execute `git checkout v{{site.ALLUXIO_RELEASED_VERSION}}` depois de `cd alluxio`.

O sistema de construção Maven busca dependências, compila códigos fonte, roda unidades de teste e
empacota o sistema. Se esta é a primeira vez que você está construindo o projeto, pode demorar um tempo
para baixar todas as dependências. Entretanto, as construções subsequentes serão mais rápidas.

Assim que o Alluxio estiver pronto, você pode iniciar este com:

{% include Common-Commands/start-alluxio.md %}

Para verificar se o Alluxio está rodando, você pode acessar [http://localhost:19999](http://localhost:19999)
ou checar os `logs` dentro do diretório `alluxio/logs`. Você também pode executar um simples programa de teste:

{% include Common-Commands/runTests.md %}

Você deverá estar apto para ver os resultados, semelhantes aos resultados seguintes:

```bash
/default_tests_files/BasicFile_STORE_SYNC_PERSIST has been removed
2015-10-20 23:02:54,403 INFO   (ClientBase.java:connect) - Alluxio client (version 1.0.0) is trying to connect with FileSystemMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,422 INFO   (ClientBase.java:connect) - Client registered with FileSystemMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,460 INFO   (BasicOperations.java:createFile) - createFile with fileId 1476395007 took 65 ms.
2015-10-20 23:02:54,557 INFO   (ClientBase.java:connect) - Alluxio client (version 1.0.0) is trying to connect with BlockMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,558 INFO   (ClientBase.java:connect) - Client registered with BlockMaster master @ localhost/127.0.0.1:19998
2015-10-20 23:02:54,590 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.31.242:29998
2015-10-20 23:02:54,654 INFO   (FileUtils.java:createStorageDirPath) - Folder /Volumes/ramdisk/alluxioworker/6601007274872912185 was created!
2015-10-20 23:02:54,657 INFO   (LocalBlockOutStream.java:<init>) - LocalBlockOutStream created new file block, block path: /Volumes/ramdisk/alluxioworker/6601007274872912185/1459617792
2015-10-20 23:02:54,658 INFO   (WorkerClient.java:connect) - Connecting local worker @ /192.168.31.242:29998
2015-10-20 23:02:54,754 INFO   (BasicOperations.java:writeFile) - writeFile to file /default_tests_files/BasicFile_STORE_SYNC_PERSIST took 294 ms.
2015-10-20 23:02:54,803 INFO   (BasicOperations.java:readFile) - readFile file /default_tests_files/BasicFile_STORE_SYNC_PERSIST took 47 ms.
Passed the test!
```

Você pode parar o sistema executando:

{% include Common-Commands/stop-alluxio.md %}

## Unidades de Teste

Para rodar todas as unidades de testes:

```bash
$ mvn test
```

Para rodar todas as unidades de teste com um armazenamento inferior (`under storage`) diferente do `filesystem`
local, execute:

```bash
$ mvn test [ -Dhadoop.version=x.x.x ] [ -P<under-storage-profile> ]
```

Atualmente, os valores suportados para `<under-storage-profile>` são:

```
Not Specified # [Default] Tests against local file system
swiftTest     # Tests against a simulated Swift cluster
hdfsTest      # Tests against HDFS minicluster
glusterfsTest # Tests against GlusterFS
s3Test        # Tests against Amazon S3 (requires a real S3 bucket)
ossTest       # Tests against Aliyun OSS (requires a real OSS bucket)
gcsTest       # Tests against Google Cloud Storage (requires a real GCS bucket)
```

Para ter a saída dos `logs` direcionados para o STDOUT, adicione a instrução a seguir para
o commando `mvn`:

```properties
-Dtest.output.redirect=false -Dalluxio.root.logger=DEBUG,CONSOLE
```

## Suporte de Distribuições

Para construir o Alluxio sobre qualquer versão diferente do `hadoop`, você apenas precisa mudar o
`hadoop.version`.

### Apache

Todas as construções principais são do Apache portanto todas as versões Apache podem ser utilizadas
diretamente:

```properties
-Dhadoop.version=2.2.0
-Dhadoop.version=2.3.0
-Dhadoop.version=2.4.0
```

### Cloudera

Para construir sobre uma versão do Cloudera, apenas mencione a versão como `$apacheRelease-cdh$cdhRelease`:

```properties
-Dhadoop.version=2.3.0-cdh5.1.0
-Dhadoop.version=2.0.0-cdh4.7.0
```

### MapR

Para construir sobre uma versão MapR:

```properties
-Dhadoop.version=2.7.0-mapr-1607
-Dhadoop.version=2.7.0-mapr-1602
-Dhadoop.version=2.7.0-mapr-1506
-Dhadoop.version=2.3.0-mapr-4.0.0-FCS
```

### Pivotal

Para construir sobre uma versão Pivotal release, apenas mencione a versão como `$apacheRelease-gphd-$pivotalRelease`:

```properties
-Dhadoop.version=2.0.5-alpha-gphd-2.1.1.0
-Dhadoop.version=2.2.0-gphd-3.0.1.0
```

### Hortonworks

Para construir sobre uma versão Hortonworks, apenas mencione a versão como `$apacheRelease.$hortonRelease`:

```properties
-Dhadoop.version=2.1.0.2.0.5.0-67
-Dhadoop.version=2.2.0.2.1.0.0-92
-Dhadoop.version=2.4.0.2.1.3.0-563
```

## Configurações de Sistema

Algumas vezes, você precisará tratar com pequenas configurações de sistemas para poder garantir que as
unidades de teste concluam Uma configuração comum que pode ser necessário é de configurar o `ulimit`.

### Mac

Para aumentar a quantidade de arquivos e processos permitidos, execute o passo a seguir:

```bash
$ sudo launchctl limit maxfiles 32768 32768
$ sudo launchctl limit maxproc 32768 32768
```

Também é recomendado que exclua o clone local do Alluxio a partir da indexação do Spotlight. Caso
contrário, o seu Mac pode travar constantemente tentando re-indexar o `file system` durante as
unidades de teste. Para efetuar esta alteração, vá para `Preferências do Sistema > Spotlight > Privacidade`,
clique no botão `+`, navegue para o diretório que contém o seu clone do Alluxio e clique em `Escolher` para
adicionar isso na lista de exclusão.
