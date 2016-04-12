---
layout: global
title: Visão Geral
group: Home
---

Alluxio, o primeiro sistema distribuído de armazenamento virtual centralizado em memória do mundo, 
unifica o acesso ao dado às estruturas computacionais e sistemas de armazenamento inferiores. 
Aplicações somente necessitam se conectar com o Alluxio para acessar o dado armazenado em quaisquer 
`underlying storage systems`. Adicionalmente, a arquitetura centralizada em memória do Alluxio 
permite o acesso ao dado em ordens de magnitude mais rápidas que as soluções atuais.

Em um ecosistema `big data`, o Alluxio posiciona-se entre as estruturas computacionais e as rotinas, 
como o `Apache Spark`, `Apache MapReduce` e o `Apache Flink`, e também como vários outros tipos de 
sistemas de armazenamento, como `Amazon S3`, `OpenStack Swift`, `GlusterFS, HDFS`, `Ceph` ou `OSS`. O Alluxio 
provoca importantes aprimoramentos de desempenho para o ecosistema, por exemplo, o 
[Baidu](https://www.baidu.com) utiliza o Alluxio para aprimorar a velocidade de vazão das análises 
de seus dados em até [30 vezes](http://www.alluxio.com/assets/uploads/2016/02/Baidu-Case-Study.pdf).
A Barclays tornou possível acelerar suas rotinas de 
[horas para segundos](https://dzone.com/articles/Accelerate-In-Memory-Processing-with-Spark-from-Hours-to-Seconds-With-Tachyon) 
com o Alluxio.
Além do desempenho, o Alluxio abri caminho para novas cargas de trabalho com dados armazenados em 
sistemas de armazenamento tradicionais. Usuários podem rodar o Alluxio utilizando seu 
`standalone cluster`, por exemplo, um `Amazon EC2` ou iniciando o Alluxio com `Apache Mesos` ou `Apache Yarn`.

O Alluxio é compatível com o `Hadoop`. Isto significa que programas existentes em `Spark` e `MapReduce` 
podem rodar com o Alluxio sem nenhuma alteração no código. O projeto é um código aberto 
([Apache License 2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE)) e está implementado em 
várias companhias. É um dos projetos de código aberto que está crescendo mais rápido, em menos de três anos 
de sua história em código aberto, o Alluxio já atraiu mais de 
[160 contribuidores](https://github.com/alluxio/alluxio/graphs/contributors) de mais de 50 instituições, 
incluindo [Alibaba](http://www.alibaba.com), [Alluxio](http://www.alluxio.com/),
[Baidu](https://www.baidu.com), [CMU](https://www.cmu.edu/), [IBM](https://www.ibm.com),
[Intel](http://www.intel.com/), [NJU](http://www.nju.edu.cn/english/), [Red Hat](https://www.redhat.com/),
[UC Berkeley](https://amplab.cs.berkeley.edu/) e [Yahoo](https://www.yahoo.com/).
O projeto é um camada de armazenamento do `Berkeley Data Analytics Stack` 
([BDAS](https://amplab.cs.berkeley.edu/bdas/)) e também faz parte da 
[distribuição Fedora](https://fedoraproject.org/wiki/SIGs/bigdata/packaging).

[Github](https://github.com/alluxio/alluxio/) |
[Lançamentos](http://alluxio.org/releases/) |
[Downloads](http://alluxio.org/downloads/) |
[Documentos de Usuários](Getting-Started.html) |
[Documentos de Desenvolvedores](Contributing-to-Alluxio.html) |
[Meetup Group](https://www.meetup.com/Alluxio/) |
[JIRA](https://alluxio.atlassian.net/browse/ALLUXIO) |
[Lista de Email de Usuário](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users) |
[Distribuído Por](Powered-By-Alluxio.html)

# Funcionalidades Atuais

* **[Flexible File API](File-System-API.html)** A `API` nativa do Alluxio é similar a classe 
``java.io.File``, provendo interfaces de `InputStream` e `OutputStream` interfaces, assim como 
suporte eficiente para `I/O` mapeado em memória. Nós recomendamos utilizar esta `API` para obter 
melhor desempenho com o Alluxio. Alternativamente, o Alluxio provê compatibilidade com a interface 
de `file system` do `Hadoop`, permitindo o `Hadoop MapReduce` e `Spark` em utilizar o Alluxio ao 
invés do `HDFS`.

* **Pluggable Under Storage** Para prover tolerância a falha, o Alluxio efetua `checkpoints`
dos dados em memória para o sistema de armazenamento inferior. Este possui uma interface genérica 
para conectar diferentes sistemas de armazenamento inferior, de forma fácil. Nós atualmente 
suportamos `Amazon S3`, `OpenStack Swift`, `Apache HDFS`, `GlusterFS` e `file system` local em módo 
`single-node`. O suporte para demais `file systems` está vindo.

* **[Armazenamento por Nível](Tiered-Storage-on-Alluxio.html)** Com o Armazenamento Por Nível, o 
Alluxio pode gerenciar `SSDs` e `HDDs` além da memória, permitindo conjunto de dados maiores serem 
armazenados no Alluxio. Os dados serão automaticamente gerenciados entre os diferentes níveis e 
um conceito de fixação permite o controle direto do usuário.

* **[Namespace Unificado](Unified-and-Transparent-Namespace.html)** O Alluxio habilita o 
gerenciamento de dado efetivamente entre diferentes sistemas de armazenamento através da 
funcionalidade `mount`. Além do mais, este garante uma nomenclatura transparente para os nomes dos 
arquivos e hierarquia de diretórios para objetos criados no Alluxio serão preservados quando 
estes objetos forem persistidos no sistema de armazenamento inferior.

* **[Linhagem](Lineage-API.html)** O Alluxio pode alcançar altas vazões de escritas sem comprometer 
a tolerância a falha através da utilização de `lineage` (linhagem) onde o resultado perdido 
pode ser recuperado através da re-execução de rotinas que criaram o resultado. Com a linhagem, 
as aplicações escrevem resultados dentro da memória e o Alluxio efetua `checkpoints` periodicamente 
dentro de um `file system` do armazenamento inferior de forma assíncrona. Em caso de falhas, o 
Alluxio inicia rotinas de re-computação para restaurar os arquivos perdidos.

* **[Interface de Usuário Web](Web-Interface.html) & [Linha de Comando](Command-Line-Interface.html)** 
Os usuários podem navegar pelo `file system` facilmente através da `web UI`. No modo `debug`, os 
administradores pode visualizar informações detalhadas de cada arquivo, incluindo localização, 
caminho de `checkpoint` e etc. Os usuários também podem utilizar o ``./bin/alluxio fs`` para 
interagir com o Alluxio, exemplo, copiar um dado para dentro e fora do `file system`.

# Iniciando

Para possuir o Alluxio rodando o mais rápido possível, dê uma olhada em nossa página 
[Iniciando](Getting-Started.html) que irá informar como implementar o Alluxio com alguns comandos 
básicos em um ambiente local.

# Downloads

Você pode baixar as versões de lançamento do Alluxio a partir da página 
[Project Downloads](http://alluxio.org/downloads). Cada lançamento vem com um binário compatível com 
diversas versões do `Hadoop`. Se você quer construir o projeto a partir do código fonte, veja a página 
[Construindo A Partir da Documentação Master Branch](Building-Alluxio-Master-Branch.html).
