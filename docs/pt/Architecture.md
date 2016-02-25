---
layout: global
title: Visão Geral da Arquitetura
nickname: Visão Geral da Arquitetura
group: User Guide
priority: 1
---

* Table of Contents
{:toc}

# Onde o Alluxio Se Encaixa

Devido ao modelo centralizado em memória do Alluxio e ser o ponto central de acesso, Alluxio mantém 
um lugar único no ecosistema de Big Data, residindo entre o armazenamento tradicional como o Amazon S3, 
Apache HDFS ou OpenStack Swift, e estruturas de computação e aplicações como Apache Spark ou Hadoop 
MapReduce. Para aplicações de usuários e estruturas de computação, Alluxio é a camada mais baixa que 
gerencia o acesso ao dado e ao rápido armazenamento destes, facilitando o compartilhamento de dados 
e a localidade de tarefas, independentemente de onde estas estiverem executando na mesma estrutura 
computacional. Como resultado, Alluxio pode aumentar em uma ordem de magnitude a velocidade de tarefas 
para aplicações Big Data enquanto provê uma interface comum de acesso aos dados. O Alluxio mantém 
transparente a integração do sistema de armazenamento inferior (`under storage systems`) para as
aplicações, qualquer `under storage` pode suportar uma gama de aplicações e estruturas sendo 
executadas em cima do Alluxio. Acompanhado com um potencial que permite montar múltiplos `under storage`, 
Alluxio pode servir como uma camada unificada para as variadas origens de dados. 

![Stack]({{site.data.img.stack}})

# Componentes Alluxio

O modelo do Alluxio utiliza um único `master` e múltiplos `workers`. Em um alto nível, o Alluxio pode ser 
divido em três componentes:  o [master](#master); os [workers](#worker); e os [clients](#client). Os 
`master` e `workers` juntos formam o `Alluxio Server`, estes são os componentes que um administrador do
sistema irá utilizar para gerenciar e manter o mesmo. Os `clients` são, genericamente, as aplicações, 
como as tarefas Spark ou MapReduce, ou usuários utilizando linha de comando (`CLI`) do Alluxio. Estes 
usuários irão apenas precisar de uma porção do `client` do Alluxio.

### Master

Aluxio pode ser disponibilizado em dois modos de `master`, [único master](Running-Alluxio-Locally.html)
ou [modo de tolerância a falha](Running-Alluxio-Fault-Tolerant-on-EC2.html). O `master` é primeiramente
responsável por gerenciar a estrutura de metadados global do sistema, como exemplo, a estrutura
árvore de diretório. Os `clients` podem interagir com o `master` para leitura e modificação do metadado.
Além disso, todos os `workers` efetuam periodicamente `heartbeats` para o `master` a fim de manter a 
participação destes no cluster. O `master` não inicia comunicação com nenhum componente, este apenas 
interage com outros componentes respondendo as requisições destes.

### Worker 

Alluxio `workers` são responsáveis por [gerenciar os recursos locais](Tiered-Storage-on-Alluxio.html) 
alocados para o Alluxio. Estes recursos podem ser memórias locais, discos SSDs ou HDs, que são 
escolhidos e configurados pelo usuário. Estes `workers` armazenam dados como blocos e respondem requisições 
de `clients` para leitura ou escrita de dados através da leitura ou escrita de novos blocos. Entretanto, 
o `worker` é apenas responsável pelo dado nestes blocos, o mapeamento atual de um arquivo para blocos 
é armazenado somente no `master`.

### Client

O Alluxio `client` provê um elo de comunicação para interagir com o `Alluxio Server`. Este provê um 
[file system API](File-System-API.html) que se comunica com o `master` para tratar das operações de 
metadados e coordena a leitura e escrita de dados existentes com os `workers` no Alluxio. O dado que
existe em um `under storage system` que não está disponível no Alluxio, é acessado
diretamente através do `client` deste.
