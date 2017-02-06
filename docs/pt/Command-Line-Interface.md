---
layout: global
title: Interface da Linha de Comando
group: Features
priority: 0
---

* Table of Contents
{:toc}

A interface da linha de comando do Alluxio fornece operações básicas de `file system` aos usuários.
Você pode invocar a ferramenta de linha de comando usando:

{% include Command-Line-Interface/alluxio-fs.md %}

Todas as variáveis de caminhos do comando `fs` devem iniciar com

{% include Command-Line-Interface/alluxio-path.md %}

Ou, se o cabeçalho não for informado, serão utilizados a porta e o servidor configurados no arquivo
de vairáveis de ambiente.

    /<path>

## Caracter Asterisco

A maioria dos comandos que necessitam de diretórios permitem a utilização do argumento asterisco (curinga)
para atribuição fácil quando desejar referenciar valores distintos. Por exemplo:

{% include Command-Line-Interface/rm.md %}

O comando do exemplo anterior irá deletar tudo dentro do diretório `data` com o prefixo `2014`.

Atente que alguns terminais poderão tentar englobar o diretório informado, causando erros estranhos
(Nota: o número 21 pode ser diferente e estar relacionado ao número de arquivos no seu `file system`
local):

{% include Command-Line-Interface/rm-error.md %}

Como solução alternativa, você pode desabilitar o englobamento, também conhecido como `globbing`, (dependendo
do seu terminal digite `set -f`) ou tratando (escapando ou `escape`) o asterisco, conforme exemplo:

{% include Command-Line-Interface/escape.md %}

Atente à dupla tratativa que ocorre devido ao `shell script`, que irá executar eventualmente um programa `java`,  
deve ter uma tratativa de parâmetros finais (`cat /\\*`).

## Lista de Operações

<table class="table table-striped">
  <tr><th>Operation</th><th>Syntax</th><th>Description</th></tr>
  {% for item in site.data.table.operation-command %}
    <tr>
      <td>{{ item.operation }}</td>
      <td>{{ item.syntax }}</td>
      <td>{{ site.data.table.en.operation-command[item.operation] }}</td>
    </tr>
  {% endfor %}
</table>

## Exemplos de Utilização

### cat
O comando `cat` imprime o conteúdo completo de um arquivo no Alluxio para o console. Este pode ser
útil para visualizar se arquivo é o que o usuário espera. Se você deseja copiar o arquivo para o
seu `file system` local, o comando `copyToLocal` deve ser utilizado.

Como exemplo, quando você está rodando uma nova rotina, o `cat` pode ser útil como uma forma rápida
de checar o conteúdo do arquivo:

{% include Command-Line-Interface/cat.md %}

### chgrp
O comando `chgrp` modifica o grupo de um arquivo ou diretório no Alluxio. O Alluxio suporta autorização
de arquivo com `POSIX`. O grupo é uma entidade autorizável no modelo de permissão de arquivo `POSIX`.
O proprietário do arquivo ou o super usuário podem executar este comando para modificar o grupo de um
arquivo ou diretório.

Adicionando a opção `-R` pode alterar o grupo de arquivos ou diretórios filhos de forma recursiva.

Como exemplo, o `chgrp` pode ser utilizado como uma forma rápida para mudar o grupo de um arquivo:

{% include Command-Line-Interface/chgrp.md %}

### chmod
O comando `chmod` modifica a permissão de um arquivo ou diretório no Alluxio. Atualmente o modo
octal é suportado: o formato numérico aceita três dígitos octal que referenciam para as permissões
de proprietário do arquivo, o grupo e os outros usuários. Aqui está a tabela de mapeamento da permissão
baseada em número:

Adicionando a opção `-R` pode alterar a permissão de arquivos ou diretórios filhos de forma recursiva.

<table class="table table-striped">
  <tr><th>Number</th><th>Permission</th><th>rwx</th></tr>
  {% for item in site.data.table.chmod-permission %}
    <tr>
      <td>{{ item.number }}</td>
      <td>{{ item.permission }}</td>
      <td>{{ item.rwx }}</td>
    </tr>
  {% endfor %}
</table>

Como exemplo, o `chmod` pode ser utilizado como uma forma rápida para mudar a permissão de um arquivo:

{% include Command-Line-Interface/chmod.md %}

### chown
O comando `chown` modifica o proprietário de um arquivo ou diretório no Alluxio. Por razões óbvias de
segurança, o proprietário do arquivo só pode ser alterado pelo super usuário.

Adicionando a opção `-R` pode alterar o proprietário de arquivos ou diretórios filhos de forma recursiva.

Como exemplo, o `chown` pode ser utilizado como uma forma rápida para mudar a propriedade de um arquivo:

{% include Command-Line-Interface/chown.md %}

### copyFromLocal
O comando `copyFromLocal` copia o um arquivo no `file system` local para dentro do Alluxio. Se o nó que
executar o comando possuir um Alluxio `worker`, o dado vai estar disponível neste `worker`. Caso
contrário, o dado estará localizado em um servidor remoto aleatório que possui um Alluxio `worker`
em execução. Se um diretório for especificado, o diretório e todo o seu conteúdo será carregado,
recursivamente.

Como exemplo, o `copyFromLocal` pode ser utilizado com uma forma rápida de carregar dados no sistema para
processamento:

{% include Command-Line-Interface/copyFromLocal.md %}

### copyToLocal
O comando `copyToLocal` copia o conteúdo de um arquivo no Alluxio para dentro do seu `file system` local.
Se um diretório for especificado, o diretório e todo o seu conteúdo será carregado, recursivamente.

Como exemplo, o `copyToLocal` pode ser utilizado com uma forma rápida de baixar os dados resultados de uma
rotina para investigação ou depuração.

{% include Command-Line-Interface/copyToLocal.md %}

### count
O comando `count` informa um número de arquivos ou diretórios que correspondem a um prefixo assim como o
tamanho total dos arquivos. O `Count` funciona de forma recursiva e leva em consideração diretórios e
arquivos aninhados. Este comando é melhor utilizado quando o usuário possui alguma convenção de nomenclatura
definida para seus arquivos.

Como exemplo, se os arquivos de dados estão armazenados por data, o `count` pode ser utilizado para determinar
o número de arquivos de dados e seus tamanhos totais por qualquer dia, mês ou ano.

{% include Command-Line-Interface/count.md %}

### du
O commando `du` informa o tamanho de um arquivo. Se um diretório for especificado, este resultado será a
soma do tamanho de todos os arquivos do diretório, inclusive seus diretórios filhos.

Como exemplo, se o espaço no Alluxio estiver acima do utilizado, o `du` pode ser utilizado para detectar quais
diretórios estão utilizando todo o espaço.

{% include Command-Line-Interface/du.md %}

### fileInfo
O comando `fileInfo` descarrega a representação das informações do arquivo na console. É necessário para
auxiliar super usuários na análise de seu sistema. Geralmente, visualizam a informação do arquivo na
interface `web` do usuário, que é muito mais fácil de entender.

Como exemplo, o `fileInfo` pode ser utilizado para analisar a localização dos blocos de um arquivo. Isto é
útil quando está sendo buscado atingir uma melhor localização na execução das cargas de trabalho de computação.

{% include Command-Line-Interface/fileInfo.md %}

### free
O comando `free` envia requisições ao `master` para que sejam desalojados todos os blocos de um arquivo dos
Alluxio `workers`. Se o argumento do comando for um diretório, este irá trabalhar de forma recursiva para
todos os arquivos. Esta requisição não poderá ser efetuada de imediato se os leitores estiverem utilizando
os blocos do arquivo neste momento. O comando retornará imediatamente depois que a requisição estiver
confirmada pelo `master`. Atente que o `free` não deleta nenhum arquivo do armazenamento inferior e ele
apenas surte efeito nos dados armazenados no Alluxio. Alem disso, os metadados não serão afetados por
esta operação, significando que o arquivo expurgado ainda irá ser visualizado pelo comando `ls`.

Como exemplo, o `free` pode ser utilizado para manualmente gerenciar os dados em `cache` do Alluxio.

{% include Command-Line-Interface/free.md %}

### getCapacityBytes
O comando `getCapacityBytes` retorna o número máximo em `bytes` que o Alluxio está configurado para armazenar.

Como exemplo, `getCapacityBytes` pode ser utilizado para verificar se o `cluster` está configurado
conforme o esperado.

{% include Command-Line-Interface/getCapacityBytes.md %}

### getUsedBytes
O comando `getUsedBytes` retorna o número máximo em `bytes` utilizados no Alluxio.

Como exemplo, o `getUsedBytes` pode ser utilizado para monitorar a atual capacidade utilizada no seu `cluster`.

{% include Command-Line-Interface/getUsedBytes.md %}

### load
O comando `load` move dados do `under storage system` para dentro do Alluxio. Se já existe um `worker`
na máquina que o comando está sendo executado, o dado será carregado por este `worker`. Caso contrário,
um `worker` aleatório será selecionado para servir este dado. O `load` não irá operar se o dado já
estiver dentro da memória do Alluxio. Se o `load` for executado referenciando um diretório, os arquivos
dentro deste serão carregados de maneira recursiva.

Como exemplo, o `load` pode ser utilizado com o intuito de buscar dados para tarefas analíticas.

{% include Command-Line-Interface/load.md %}

### loadMetadata
O comando `loadMetadata` pesquisa o sistema de armazenamento inferior para qualquer arquivo ou
diretório correspondente ao caminho informado e depois cria uma referência do arquivo no Alluxio apoiado
por este arquivo no `under storage system`. Somente os metadados, como o nome do arquivo e tamanho, são
carregados desta forma e nenhuma transferencia de dados ocorre.

Como exemplo, o `loadMetada` pode ser utilizado quando há a necessidade do resultado de rotinas de
outros sistemas diretamente para o armazenamento inferior (ignorando o Alluxio) e a aplicação que utiliza
o Alluxio precisa dos resultados destes outros sistemas.

{% include Command-Line-Interface/loadMetadata.md %}

### location
O comando `location` retorna o endereço de todos os Alluxio `workers` que contém blocos pertencentes a um
determinado arquivo.

Como exemplo, o `location` pode ser utilizado para depurar a localidade de dados quando se executam
rotinas que utilizam uma estrutura de computação.

{% include Command-Line-Interface/location.md %}

### ls
O comando `ls` lista todos os filhos diretos de um diretório e informa: o tamanho do arquivo;
última data de modificação; e estado do arquivo em memória. Utilizando o `ls` em apenas um arquivo, você  
visualizará somente as informações deste arquivo.

Adicionando a opção `-R` também lista os diretórios filhos de forma recursiva.

Como exemplo, o `ls` pode ser utilizado para navegar pelo `file system`.

{% include Command-Line-Interface/ls.md %}

### mkdir
O comando `mkdir` cria um novo diretório no Alluxio. Este pode ser recursivo e irá criar todos os
diretórios pais que não existem. Atente que o diretório criado não será criado dentro do
`under storage system` até que o arquivo no diretório seja marcado como persistente no armazenamento
subjacente. Utilizando o `mkdir` em um caminho inválido ou que já existe irá falhar a execução.

Como exemplo, o `mkdir` pode ser utilizado pelo administrador para definição da estrutura de diretórios.

{% include Command-Line-Interface/mkdir.md %}

### mount
O comando `mount` vincula um caminho no armazenamento inferior com um do Alluxio e os arquivos e diretórios
criados no Alluxio serão suportados pelo arquivo ou diretório correspondente no caminho do armazenamento
inferior. Para maiores detalhes, veja o [Namespace Unificado](Unified-and-Transparent-Namespace.html).

Como exemplo, o `mount` pode ser utilizado para fazer o dado de um outro armazenamento disponível no Alluxio.

{% include Command-Line-Interface/mount.md %}

### mv
O comando `mv` move um arquivo ou diretório para outro caminho dentro do Alluxio. O caminho destino informado
não deve existir e ou deve ser um diretório. Se for um diretório, o arquivo ou diretório será depositado
como um filho deste diretório. O comando é uma operação de metadados e não afeta os blocos de dados do arquivo.
Este não pode ser utilizado entre diferentes pontos de montagens de `under storage systems`.

Como exemplo, o `mv` pode ser utilizado para mover dados antigos de um diretório para um diretório histórico.

{% include Command-Line-Interface/mv.md %}

### persist
O comando `persist` persiste os dados no Alluxio para o `under storage system`. Este é uma operação que envolve
dados e irá depender do tamanho do arquivo. Depois que for concluído o comando, o arquivo no Alluxio será
suportado pelo arquivo no armazenamento inferior, mantendo o arquivo válido mesmo que os blocos no
Alluxio forem expurgados ou perdidos.

Como exemplo, o `persist` pode ser utilizado depois que filtrarem um conjunto de arquivos temporários para
outros contendo dados úteis.

{% include Command-Line-Interface/persist.md %}

### pin
O comando `pin` fixa um arquivo ou diretório no Alluxio. Esta é uma operação de metadados e não irá causar
nenhuma carga de dados no Alluxio. Se o arquivo estiver fixado, todos os blocos pertencentes ao arquivo
nunca serão expurgados por um Alluxio `worker`. Se existem vários arquivos fixados, os Alluxio `workers`
poderão ficar com uma baixa capacidade de armazenamento e não efetuar a fixação de demais arquivos.

Como exemplo, o `pin` pode ser utilizado manualmente para garantir performance, se o administrador compreender  
bem a sua carga de trabalho e rotinas.

{% include Command-Line-Interface/pin.md %}

### report
O comando `report` marca um arquivo como perdido para o Alluxio `master`. Este comando só deve ser
utilizado com arquivos criados utilizando a [Lineage API](Lineage-API.html). Marcando um arquivo como
perdido, irá fazer com que o `master` agende uma rotina de computação para gerar o arquivo novamente.

Como exemplo, o `report` pode ser utilizado para forçar uma rotina de computação para gerar o arquivo novamente.

{% include Command-Line-Interface/report.md %}

### rm
O comando `rm` remove um arquivo de um diretório Alluxio e do `under storage system`. O arquivo estará
indisponível imediatamente após o comando retornar porém os dados deste serão apagados posteriormente.

Adicionando a opção `-R` pode apagar todo o conteúdo do diretório, inclusive o próprio diretório.

Como exemplo, o `rm` pode ser utilizado para remover arquivos temporários que não são mais necessários.

{% include Command-Line-Interface/rm2.md %}

### setTtl
O comando `setTtl` define um tempo de vida, em milissegundos, para um arquivo. O arquivo será automaticamente
deletado uma vez que a data atual for maior que a soma do tempo de vida e da data de criação deste arquivo.
Este comando apaga o arquivo tanto do Alluxio quanto do `under storage system`.

Como exemplo, o `setTtl` pode ser utilizado para limpar arquivos que o administrador entender serem
desnecessários depois de um período.

{% include Command-Line-Interface/setTtl.md %}

### tail
O comando `tail` visualiza na console o último 1 `KB` de dados de um arquivo.

Como exemplo, o `tail` pode ser utilizado para verificar se o conteúdo de um arquivo resultado de uma rotina
possui ou não o formato esperado.

{% include Command-Line-Interface/tail.md %}

### touch
O comando `touch` cria um arquivo de 0 `byte`. Arquivos criados com o `touch` não podem ser sobrescritos
e são úteis como `flags`.

Como exemplo, o `touch` pode ser usado para criar um arquivo no diretório que significa a finalização
de uma análise efetuada.

{% include Command-Line-Interface/touch.md %}

### unmount
O comando `umount` desvincula um caminho no Alluxio com um diretório do armazenamento inferior. Os metadados
do Alluxio que referenciam este ponto de montagem, serão removidos junto com todos os blocos porém
o `under storage system` irá manter os metadados e os dados. Veja o
[Namespace Unificado](Unified-and-Transparent-Namespace.html) para maiores detalhes.

Como exemplo, o `unmount` pode ser utilizado para remover um `under storage system` quando os usuários não
precisam mais daquele sistema.

{% include Command-Line-Interface/unmount.md %}

### unpin
O comando `unpin` desatrela um arquivo ou diretório que estava fixado no Alluxio. Esta é uma operação de
metadados e não irá expurgar ou apagar nenhum bloco de dados. Uma vez que o arquivo for desatrelado do
Alluxio, os seus dados podem ser expurgados pelos Alluxio `workers` que contém os blocos.

Como exemplo, o `unpin` pode ser utilizado quando o administrador sabe que existirá uma alteração no
modelo de acesso ao dado.

{% include Command-Line-Interface/unpin.md %}

### unsetTtl
O comando `unsetTtl` irá remover o tempo de vida de um arquivo no Alluxio. Esta é uma operação de metadados
e não serão expurgados ou armazenados blocos no Alluxio. O tempo de vida de um arquivo pode ser definido
posteriormente com o `setTtl`.

Como exemplo, o `unsetTtl` pode ser removido se um arquivo comum necessita de gerenciamento manual devido a
casos especiais.

{% include Command-Line-Interface/unsetTtl.md %}
