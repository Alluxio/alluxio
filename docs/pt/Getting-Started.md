---
layout: global
title: Iniciando
group: User Guide
priority: 0
---

* Table of Contents
{:toc}

### Configurando o Alluxio

O Alluxio pode ser configurado de várias modos. A configuração mais simples para novos usuários 
é [rodar o Alluxio localmente](Running-Alluxio-Locally.html). Para experimentar a configuração 
com um `cluster`, veja os tutorias [Virtual Box](Running-Alluxio-on-Virtual-Box.html) e 
[Amazon AWS](Running-Alluxio-on-EC2.html).

### Configurando o Under Storage (Armazenamento Inferior)

O Alluxio pode ser visto como uma camada de troca de dados e beneficia por ter o suporte de 
um armazenamento persistente confiável. Dependendo do ambiente de produção, diferentes tipos 
de armazenamentos podem ser escolhidos. O Alluxio pode ser integrado com qualquer um. Atualmente, 
os que são suportados são: [Amazon S3](Configuring-Alluxio-with-S3.html),
[OpenStack Swift](Configuring-Alluxio-with-Swift.html);
[GlusterFS](Configuring-Alluxio-with-GlusterFS.html); e
[Apache HDFS](Configuring-Alluxio-with-HDFS.html).

### Configurando uma Aplicação

O Alluxio provê uma [interface de file system](File-System-API.html) para permitir que as aplicações 
interajam com os dados armazenados no Alluxio. Se você quer que uma aplicação escreva diretamente 
no topo do Alluxio, simplesmente adicione a dependencia `alluxio-core-client` no seu programa. Por 
exemplo, se uma aplicação está compilada através do `Maven`:

{% include Getting-Started/config-application.md %}

Um conjunto especial de aplicação que dão poder ao Alluxio são as estruturas computacionais (
`computation frameworks`). Transacionando estas estruturas para utilizar o Alluxio é uma tarefa de 
pouco esforço, especialmente se o `framework` já está integrado com o interface `Hadoop FileSystem`. 
Como o Alluxio também prove uma implementação desta interface, a única modificação necessária é de 
alterar o caminho o `data path scheme` de `hdfs://master-hostname:port` para 
`alluxio://master-hostname:port`. Por exemplo, veja os tutorias
[Apache Spark](Running-Spark-on-Alluxio.html),
[Apache Hadoop MapReduce](Running-Hadoop-MapReduce-on-Alluxio.html) ou
[Apache Flink](Running-Flink-on-Alluxio.html).

### Configurando o Sistema

O Alluxio possui vários opções para ajustar o sistema visando melhor desempenho para diferentes casos 
de uso. Para uma aplicação, o Alluxio lê configurações padrões a partir do arquivo 
`alluxio-site.properties` ou de opções `Java` enviadas na linha de comando. Veja as 
[definições de configuração](Configuration-Settings.html) para maiores informações sobre ajustes 
específicos.

### Funcionalidades Adicionais

Além de prover uma camada de compartilhamento de dado com um armazenamento rápido, o Alluxio também 
possui funcionalidades úteis para desenvolvedores e administradores.

* [Interface de Linha de Comando](Command-Line-Interface.html), permite aos usuários acessar e 
manipular dados no Alluxio através de um `shell` simples fornecido no código base.
* [Coleção de Métricas](Metrics-System.html), permite administradores de monitorar com facilidade o 
estado do sistema e descobrir gargalos ou ineficiências.
* [Interface Web](Web-Interface.html), fornece uma rica visualização da representação dos dados no 
Alluxio porém é uma visualização somente de leitura.

### Funcionalidades Avançadas

Além de prover significantes ganhos de performance simplesmente por acelerar a entrada e saída de dados, 
o Alluxio também fornece as seguintes funcionalidades adicionais.

* [Armazenamento por Nível](Tiered-Storage-on-Alluxio.html), fornece recursos adicionais para o Alluxio 
gerenciar (tanto como `SSD` ou `HDD`), permitindo o compartilhamento de conjunto de registros que não 
se encaixam na memória de possuírem a vantagem de estar na arquitetura Alluxio.
* [Namespace Unificado](Unified-and-Transparent-Namespace.html), prove a habilidade aos usuários para 
gerenciar dados a partir de sistemas de armazenamento existentes e, facilmente, manusear implantações de 
onde nem todos os sistemas estão cientes do Alluxio.
* [Linhagem](Lineage-API.html), permite uma alternativa para tolerância a falha e durabilidade de dado, 
mantendo ótimos desempenho de escrita, ao invés do dispendioso processo de replicação de dado em disco.
