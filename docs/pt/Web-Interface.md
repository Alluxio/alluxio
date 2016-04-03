---
layout: global
group: Features
title: Interface Web
priority: 6
---

* Table of Contents
{:toc}

O Alluxio possui uma interface `web` amigável permitindo aos usuários observar e gerenciar o sistema 
através desta. Cada um dos `master` e `workers` possui sua própria interface de usuário `web`. A porta 
padrão para a `web` é 19999 para o `master` e 30000 para os `workers`.

# Interface Web do Alluxio Master

O Alluxio `master` oferece uma interface `web` para ajudar na gestão do sistema. A porta padrão para a 
interface `web` do Alluxio `master` é a 19999, então para acessar esta, visite `http://<MASTER IP>:19999`. 
Por exemplo, se você está rodando o Alluxio localmente, a interface `web` pode ser acessar através de  
[localhost:19999](http://localhost:19999).

A interface `web` do Alluxio `master` possui várias páginas, conforme descrito abaixo.

## Home Page

A `home page` possui a aparência abaixo:

![Alluxio Master Home Page]({{site.data.img.screenshot_overview}})

A `home page` informa a visão geral do `status` do sistema. Isto inclui as seções seguintes:

* **Alluxio Summary**

	Nível de informação do sistema Alluxio

* **Cluster Usage Summary**

	Informação de armazenamento do sistema Alluxio e também do armazenamento inferior. A utilização 
	do armazenamento do Alluxio pode estar próximo do 100% porém o armazenamento inferior não deve 
	alcançar 100% de utilização.

* **Storage Usage Summary**

	Informação de armazenamento por camada do sistema Alluxio que pode informar a quantidade de espaço 
	utilizado por camada no sistema.

## Configuration Page

Para checar as informações atuais de configuração do sistema, clique em "System Configuration" na barra 
de navegação no topo da tela.

![configurations]({{site.data.img.screenshot_systemConfiguration}})

A página de configuração possui duas seções:

* **Alluxio Configuration**

	Um mapa de todas as propriedades de configuração e seus valores definidos.

* **White List**

	Contém todos os elegíveis diretórios pré-definidos do Alluxio para serem armazenados no Alluxio. Uma 
	requisição ainda pode ser feita em um arquivo não pré-definido por um caminho que está no `white list`. 
	Apenas os arquivos da `white list` serão armazenados no Alluxio.

## Browse File System Page

Você pode navegar pelo `file system` do Alluxio através da `UI`. Quando selecionar a aba 
"Browse File System" na barra de navegação, você verá algo como isso:

![browse]({{site.data.img.screenshot_browseFileSystem}})

Os arquivos no diretório atual estão listados com o nome do arquivo, tamanho do arquivo, tamanho de 
cada bloco, percentual do dado em memória, tempo de criação e tempo de modificação. Para visualizar o 
conteúdo de um arquivo, clique neste arquivo.

## Browse In-Memory Files Page

Para navegar em todos os arquivos em memória, clique na aba "In-Memory Files" dentro da barra de navegação.

![inMemFiles]({{site.data.img.screenshot_inMemoryFiles}})

Os arquivos atualmente na camada de memória são listados com o nome do arquivo, o tamanho do arquivo, 
tamanho de cada bloco, se o arquivo foi ou não fixado em memória, a data de criação e modificação 
do arquivo.

## Workers Page

O `master` também visualiza todos os Alluxio `workers` do sistema e os mostra na aba "Workers".

![workers]({{site.data.img.screenshot_workers}})

A página dos `workers` sede uma visão geral de todos os servidores Alluxio `workers` em duas seções:

* **Live Nodes**

	Uma lista de todos os atuais `workers` que servem requisições do Alluxio. Clicando no nome do 
	`worker` irá redirecionar para a interface `web` de usuário deste `worker`.

* **Dead Nodes**

	Ima lista de todos os `workers` declarados como "mortos" pelo `master`, geralmente, devido a 
	uma longa espera de `timeout` de `heartbeat` do `worker`. Possíveis causas incluem o `reboot` 
	do servidor ou falhas na rede.
    
## Master Metrics 

Para acessar a seção de métricas do `master`, clique na aba “Metrics” dentro da aba de navegação.

![masterMetrics]({{site.data.img.screenshot_masterMetrics}})

Esta seção informa todas as métricas do `master`. Isto inclui as seções seguintes:

* **Master Gauges**

	Avaliação geral do `master`.

* **Logical Operation**

	Número de operações efetuadas.

* **RPC Invocation**

	Número de invocações `RPC` por operação.

# Interface Web dos Alluxio Workers

Cada Alluxio `worker` também oferece uma interface `web` para prover informações do `worker`. A porta 
padrão da interface `web` é 30000 e pode ser acessar em `http://<WORKER IP>:30000`. Por exemplo, 
se você iniciar o Alluxio locamente, a interface `web` do `worker` pode ser visualizada em 
[localhost:30000](http://localhost:30000).

## Home Page

A `home page` para a interface `web` do Alluxio `worker` é semelhante a `home page` do Alluxio 
`master` mas esta contém informação específica de um único `worker`. Sendo assim, esta possui 
seções semelhantes: **Worker Summary**, **Storage Usage Summary**, **Tiered Storage Details**.

## BlockInfo Page

Na página "BlockInfo", você pode ver os arquivos do `worker` e outras informações como tamanho 
do arquivo e em que camada este está armazenado. Se você clicar em um arquivo, você pode 
visualizar todos os blocos daquele arquivo.

## Worker Metrics 

Para acessar a seção de métricas do `worker`, clique na aba “Metrics” dentro da barra de 
navegação.

![workerMetrics]({{site.data.img.screenshot_workerMetrics}})

Esta seção visualiza todas as métricas do `worker`. Isto inclui as seções a seguir:

* **Worker Gauges**

	Avaliação geral do `worker`.

* **Logical Operation**

	Número de operações efetuadas.
