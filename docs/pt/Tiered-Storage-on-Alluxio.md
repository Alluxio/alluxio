---
layout: global
title: Armazenamento por Nível no Alluxio
nickname: Armazenamento por Nível
group: Features
priority: 4
---

* Table of Contents
{:toc}

O Alluxio suporta o armazenamento por nível que permite a este gerenciar outros tipos de armazenamento 
além de memória. Atualmente, o Armazenamento por Nível no Alluxio suporta três tipos de armazenamento 
ou níveis:

* MEM (Memória)
* SSD (Solid State Drives)
* HDD (Hard Disk Drives)

Utilizando o armazenamento por camada permite ao Alluxio armazenar mais dados dentro do sistema de uma 
única vez, já que a capacidade de memória pode ser limitada em alguns ambientes. Com o nível de 
armazenamento, o Alluxio, gerencia automaticamente os blocos entre todas os níveis configurados, então 
os usuários e administradores não precisarão gerenciar manualmente a localização dos dados. Os usuários 
podem especificar a sua própria estratégia de gerenciamento de dados através da implementação de 
[allocators](#allocators) e [evictors](#evictors). Além disso, o controle manual sobre o nível de 
armazenamento é possível, veja em [fixando arquivos](#fixando-os-arquivos).

# Utilizando o Armazenamento por Nível

Com a introdução de níveis, os blocos de dados gerenciados pelo Alluxio não estão, necessariamente, 
somente em memória. Os blocos podem estar em qualquer nível disponível. Para gerenciar a localização 
e movimentação dos blocos, o Alluxio utiliza os *allocators* e os *evictors* para posicionar e 
reorganizar os blocos entre os níveis. o Alluxio assume que as camadas estão em ordem de cima 
para baixo, baseado no desempenho de `I/O`. Sendo assim, uma configuração de armazenamento por camada 
típica definiria o topo da camada para a `MEM`, seguida pelo `SSD` e por fim o `HDD`.

## Diretórios de Armazenamento

A camada é feita de no mínimo um diretório de armazenamento. Este diretório é o caminho do arquivo 
onde os blocos do Alluxio devem ser armazenados. O Alluxio suporta a configuração de múltiplos 
diretórios para um único nível, permitindo múltiplos pontos de montagens e dispositivos de 
armazenamento para um nível particular. Por exemplo, se você possui cinco dispositivos `SSD` no 
seu Alluxio `worker`, você pode configurar o Alluxio para utilizar todos os cinco dispositivos 
para a camada `SSD`. A configuração para isso está descrito 
[abaixo](#configurando-e-habilitando-o-armazenamento-por-nível). Escolher qual diretório o dado deverá ser 
posicionado é determinado pelos [allocators](#allocators).

## Escrevendo os Dados

Quando o usuário escreve um bloco novo, este é escrito na camada do topo, por padrão. Um `allocator` 
padronizado pode ser utilizado se o comportamento padrão não estiver conforme o desejável. Se não 
existe espaço suficiente para o bloco na camada do topo, então o `evictor` é disparado a fim de 
liberar espaço para o novo bloco.

## Lendo os Dados

Lendo um bloco de dado da camada de armazenamento é similar ao padrão do Alluxio. O Alluxio irá, 
simplesmente, ler o bloco de onde já está armazenado. Se o Alluxio estiver configurado com múltiplas 
camadas, então o bloco não será, necessariamente, lido da camada do topo, já que este pode ter sido 
movido para uma camada mais baxa, transparentemente.

Lendo dados com o `AlluxioStorageType.PROMOTE` irá garantir que o dados será, primeiramente, 
transferido para a camada do topo antes que este seja lido pelo `worker`. Isto também pode ser 
utilizado como uma estratégia de gerenciamento de dados por mover, explicitamente, um dado "quente" 
para uma camada superior.

### Fixando os Arquivos

Outra maneira para o usuário controlar o posicionamento e movimentação de seus arquivos é a de fixar 
e soltar os arquivos. Quando um arquivo está fixado, os seus blocos não podem ser expurgados. 
Entretanto, os usuários ainda podem promover blocos de arquivos fixados para mover os blocos para a 
camada do topo.

Abaixo, um exemplo de como fixar um arquivo:

{% include Tiered-Storage-on-Alluxio/pin-file.md %}

Similarmente, um arquivo pode ser solto através de:

{% include Tiered-Storage-on-Alluxio/unpin-file.md %}

Já que os blocos de arquivos fixados não são mais candidatos para serem expurgados, os `clients` 
devem ter a certeza de soltar arquivos quando apropriado.

## Allocators

O Alluxio utiliza os `allocators` para escolher a localização da escrita de novos blocos. O 
Alluxio possui uma estrutura para padronizar os `allocators` mas existem algumas implementações 
padrões de `allocators`. Aqui estão os `allocators` existentes no Alluxio:

* **GreedyAllocator**

	Aloca o novo bloco no primeiro diretório de armazenamento que possuir espaço suficiente.

* **MaxFreeAllocator**

	Aloca o novo bloco no diretório de armazenamento que maior espaço livre.

* **RoundRobinAllocator**

	Aloca o novo bloco no nível mais alto que possui espaço, o diretório de armazenamento é 
	escolhido através de `round-robin`.

No futuro, estarão disponíveis `allocators` adicionais. Como o Alluxio suporta padronizar os 
`allocators`, você pode desenvolver o seu próprio `allocator` conforme a sua carga de trabalho.

## Evictors

O Alluxio utiliza os `evictors` para decidir quais blocos devem ser movimentados para um nível 
mais baixo, quando há a necessidade de liberar espaço. O Alluxio suporta `evictors` 
padronizados e as implementações destes incluem:

* **GreedyEvictor**

	Expulsa arbitrariamente os blocos enquanto o tamanho necessário não estiver disponível.

* **LRUEvictor**

	Expulsa os blocos com menores utilização enquanto o tamanho necessário não estiver disponível.

* **LRFUEvictor**

	Expulsa os blocos baseados em `LRU` e `LFU` com uma configuração de peso. Se o peso está 
	completamente com tendência ao `least-recently-used`, o comportamento será igual ao 
    `LRUEvictor`.

* **PartialLRUEvictor**

	Expulsa os blocos baseado em `LRU` mas irá escolher um diretório de armazenamento com maior
	capacidade de espaço livre e só expulsa do diretório de armazenamento.

No futuro, estarão disponíveis `evictors` adicionais. Como o Alluxio suporta padronizar os 
`evictors`, você pode desenvolver o seu próprio `evictor` conforme a sua carga de trabalho.

Quando estiver utilizando uma expulsão síncrona, é recomendado que utilize um tamanho de bloco 
peque (cerca de 64MB) para reduzir a latência da expulsão do bloco. Quando utilizar o 
[espaço reservado](#espaço-reservado), o tamanho do bloco não afeta a latência.

## Espaço Reservado

O espaço reservado faz com que o armazenamento por nível tente reservar uma porção de espaço em 
cada camada de armazenamento antes que todo o espaço estiver consumido. Isto irá melhorar o 
desempenho da explosão de escrita mas também pode prover um ganho de desempenho na escrita 
contínua quando a expulsão estiver em execução contínua. Veja a  
[seção de configuração section](#configurando-e-habilitando-o-armazenamento-por-nível) para 
saber como habilitar e configurar o espaço reservado.

# Configurando e Habilitando o Armazenamento por Nível

O armazenamento por nível pode ser habilitado no Alluxio utilizando as 
[configurações de parâmetros](Configuration-Settings.html). Por padrão, o Alluxio somente habilita 
um único nível, o de memória. Para especificar níveis adicionais, para o Alluxio, utilize as 
seguintes configurações de parâmetros:

{% include Tiered-Storage-on-Alluxio/configuration-parameters.md %}

Por exemplo, se você quer configura o Alluxio para possuir dois níveis (memória e `HD`), você pode 
usar a configurar similar à:

{% include Tiered-Storage-on-Alluxio/two-tiers.md %}

Aqui está uma explicação de um exemplo de configuração:

* `alluxio.worker.tieredstore.levels=2` configura 2 níveis no Alluxio
* `alluxio.worker.tieredstore.level0.alias=MEM` configura o primeiro nível (topo) para ser o nível 
de memória
* `alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk` define o `/mnt/ramdisk` para ser o 
caminho do arquivo do primeiro nível
* `alluxio.worker.tieredstore.level0.dirs.quota=100GB` define a quota para o `ramdisk` em `100GB`
* `alluxio.worker.tieredstore.level0.reserved.ratio=0.2` define a razão do espaço para ser reservado 
no nível do topo para ser 0.2
* `alluxio.worker.tieredstore.level1.alias=HDD` configura o segundo nível para ser um nível de `HDD`
* `alluxio.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3` configura 3 caminhos 
de arquivos diferentes para o segundo nível
* `alluxio.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB` define a quota para cada um dos 
caminhos de arquivo do segundo nível
* `alluxio.worker.tieredstore.level1.reserved.ratio=0.1` define a razão do espaço para ser reservado 
no segundo nível para ser 0.1

Existem algumas pequenas restrições para definir os níveis. Primeiro de tudo, podem haver 3 níveis 
no máximo. Também, no máximo um nível pode referenciar um nome específico. Por exemplo, no máximo 
um nível pode possuir o nome `HDD`. Se você quer que o Alluxio utilize múltiplos `hard drives` 
para a camada de `HDD`, você deve configurar utilizando múltiplos caminhos em 
`alluxio.worker.tieredstore.level{x}.dirs.path`.

Adicionalmente, as estratégias específicas do `evictor` e do `allocator` podem ser configuradas. 
Estas configurações de parâmetro são:

{% include Tiered-Storage-on-Alluxio/evictor-allocator.md %}

O espaço reservado pode ser configurado para estar habilitado ou desabilitado através de:

    alluxio.worker.tieredstore.reserver.enabled=false

# Parametros de Configuração para o Armazenamento por Nível

Estes são os parâmetros de configuração para o armazenamento por nível.

<table class="table table-striped">
<tr><th>Parameter</th><th>Default Value</th><th>Description</th></tr>
{% for item in site.data.table.tiered-storage-configuration-parameters %}
<tr>
<td>{{ item.parameter }}</td>
<td>{{ item.defaultValue }}</td>
<td>{{ site.data.table.en.tiered-storage-configuration-parameters.[item.parameter] }}</td>
</tr>
{% endfor %}
</table>
