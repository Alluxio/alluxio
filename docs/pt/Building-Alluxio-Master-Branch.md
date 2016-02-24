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

Confira o Alluxio `master branch` a partir do Github e o empacote:

{% include Building-Alluxio-Master-Branch/checkout.md %}

Se você estiver vendo o `java.lang.OutOfMemoryError: Java heap space`, por favor execute:

{% include Building-Alluxio-Master-Branch/OutOfMemoryError.md %}

Se você quer construir alguma versão particular do Alluxio, por exemplo {{site.ALLUXIO_RELEASED_VERSION}},
por favor execute `git checkout v{{site.ALLUXIO_RELEASED_VERSION}}` depois de `cd alluxio`.

O sistema de construção Maven busca dependências, compila códigos fonte, roda unidades de teste e
empacota o sistema. Se esta é a primeira vez que você está construindo o projeto, pode demorar um tempo
para baixar todas as dependências. Entretanto, as construções subsequentes serão mais rápidas.

Assim que o Alluxio estiver pronto, você pode iniciar este com:

{% include Common-Commands/start-alluxio.md %}

Para verificar se o Alluxio está rodando, você pode acessar [http://localhost:19999](http://localhost:19999)
ou checar os `logs` dentro do diretório `alluxio/logs`. Você também pode executar um simples programa:

{% include Common-Commands/runTests.md %}

Você deve estar apto para ver os resultados similares aos seguintes:

{% include Building-Alluxio-Master-Branch/test-result.md %}

Você pode parar o sistema executando:

{% include Common-Commands/stop-alluxio.md %}

# Unidades de Teste

Para rodar todas as unidades de testes:

{% include Building-Alluxio-Master-Branch/unit-tests.md %}

Para rodar todas as unidades de teste com um armazenamento inferior diferente do `filesystem`
local, execute:

{% include Building-Alluxio-Master-Branch/under-storage.md %}

Atualmente, os valores suportados para `<under-storage-profile>` são:

{% include Building-Alluxio-Master-Branch/supported-values.md %}

Para ter a saída dos `logs` direcionadas para o STDOUT, adicione a instrução a seguir para 
o commando `mvn`:

{% include Building-Alluxio-Master-Branch/STDOUT.md %}

# Suporte de Distribuições

Para construir o Allexo sobre qualquer versão diferente do hadoop, você apenas precisa mudar o
`hadoop.version`.

## Apache

Todas as construções principais são do Apache portanto todas as versão Apache podem ser utilizadas
diretamente:

{% include Building-Alluxio-Master-Branch/Apache.md %}

## Cloudera

Para construir sobre uma versão do Cloudera, apenas utilize a versão como `$apacheRelease-cdh$cdhRelease`:

{% include Building-Alluxio-Master-Branch/Cloudera.md %}

## MapR

Para construir sobre uma versão MapR:

{% include Building-Alluxio-Master-Branch/MapR.md %}

## Pivotal

Para construir sobre uma versão Pivotal release, apenas utilize a versão como `$apacheRelease-gphd-$pivotalRelease`:

{% include Building-Alluxio-Master-Branch/Pivotal.md %}

## Hortonworks

Para construir sobre uma versão Hortonworks, apenas utilize a versão como `$apacheRelease.$hortonRelease`:

{% include Building-Alluxio-Master-Branch/Hortonworks.md %}

# Configurações de Sistema

Algumas vezes, você precisará tratar com pequenas configurações de sistemas para poder garantir que as
unidades de teste passem localmente. Uma configuração comum que pode ser necessário é para configurar o
`ulimit`.

## Mac

Para aumentar a quantidade de arquivos e processos permitidos, execute o passo a seguir:

{% include Building-Alluxio-Master-Branch/increase-number.md %}

Também é recomendado que exclua o clone local do Alluxio a partir da indexação do Spotlight. Caso
contrário, o seu Mac pode travar constantemente tentando reindexar o `file system` durante as 
unidades de teste. Para fazer isso, vá para `Preferências do Sistema > Spotlight > Privacidade`, clique no botão `+`, 
navegue para o diretório que contém o seu clone do Alluxio e clique em `Escolher` para adicionar isso na 
lista de exclusão.
