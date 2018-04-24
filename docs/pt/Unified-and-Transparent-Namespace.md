---
layout: global
title: Namespace Unificado e Transparente
nickname: Namespace Unificado
group: Features
priority: 5
---

* Table of Contents
{:toc}

O Alluxio permite um efetivo gerenciamento de dados entre diferentes sistemas de armazenamento através
da utilização de uma nomeação transparente e uma `API` de ponto de montagem.

## Nomeação Transparente

A nomeação transparente mantém uma identidade entre o Alluxio `namespace` e o sistema de armazenamento 
inferior.

![transparent]({{site.data.img.screenshot_transparent}})

Quando um usuário cria objetos no Alluxio `namespace`, estes podem escolher objetos se devem ser mantidos  
no sistema de armazenamento inferior. Para objetos que serão mantidos, o Alluxio preserva o caminho do 
objeto, relativo ao do diretório no `under storage system` em que os objetos do Alluxio são armazenados. 
Por exemplo, se um usuário criar um diretório de nível superior chamado `Usuarios` com os subdiretórios 
`Alice` e `Bob`, a estrutura de diretório e nomeação será preservada no sistema de armazenamento inferior 
(exemplo, `HFDS` e `S3`). Similarmente, quando um usuário renomeia ou apaga um objeto que era mantido no 
Alluxio `namespace`, este é renomeado ou apago do sistema de armazenamento inferior.

Além disso, o Alluxio descobre, transparentemente, o conteúdo presente no sistema de armazenamento inferior 
que não foi criado através do Alluxio. Por exemplo, se o `under storage system` possui um diretório chamado 
`Dados` com os arquivos `Relatorios` e `Vendas`, todos estes não foram criados através do Alluxio, seus 
metadados serão carregados dentro do Alluxio pela primeira vez que forem acessados (exemplo, quando um 
usuário solicitar a abertura de um arquivo). O dado do arquivo não será carregado no Alluxio durante este 
processo. Para carregar o dado no Alluxio, isto pode ser definido com o `AlluxioStorageType` para `STORE` 
quando estiver lendo o dado pela primeira vez ou utilize o comando `load` do Alluxio `shell`. 

## Namespace Unificado

O Alluxio fornece uma `API` para ponto de montagem que torna possível para utilizar o Alluxio para acessar 
os dados através de múltiplas origens de dados.

![unified]({{site.data.img.screenshot_unified}})

Por padrão, o Alluxio `namespace` é montado no diretório definido pela propriedade de configuração 
`alluxio.underfs.address` do Alluxio. Este diretório identifica o armazenamento primário para o 
Alluxio. Além disso, os usuários podem utilizar a `API` de montagem para adicionar e removes origens 
de dados:

{% include Unified-and-Transparent-Namespace/mounting-API.md %}

Por exemplo, o armazenamento primário pode ser um `HDFS` e contém diretórios de usuários; o diretório 
`Dados` pode ser armazenado em um `S3 bucket`, que está montado para o Alluxio `namespace` através da 
invocação `mount(alluxio://host:port/Data, s3://bucket/directory)`.

## Exemplo

Neste exemplo, nós iremos demonstrar as funcionalidades acima. O exemplo assume que o código fonte do 
Alluxio existe dentro do diretório `${ALLUXIO_HOME}` e que existe uma instância do Alluxio em execução.

Primeiramente, vamos criar um diretório temporário dentro do `file system` local que vamos usar para o 
exemplo:

{% include Unified-and-Transparent-Namespace/mkdir.md %}

Em seguida, iremos montar o diretório criado acima dentro do Alluxio e verificar se o diretório montado 
aparece no Alluxio:

{% include Unified-and-Transparent-Namespace/mount-demo.md %}

Depois, nós iremos verificar que os metadados para os conteúdos não criados pelo Alluxio estão carregados 
dentro do Alluxio na primeira vez que o conteúdo for acessado:

{% include Unified-and-Transparent-Namespace/ls-demo-hello.md %}

Seguindo o teste, iremos criar um arquivo no diretório montado e verificar se o arquivo foi criado no 
sistema de armazenamento inferior:

{% include Unified-and-Transparent-Namespace/create-file.md %}

A seguir, iremos renomear um arquivo no Alluxio e verificar se o arquivo foi renomeado no armazenamento 
inferior:

{% include Unified-and-Transparent-Namespace/rename.md %}

Depois disso, iremos apagar o arquivo no Alluxio e verificar se o arquivo foi apagado do armazenamento 
inferior:

{% include Unified-and-Transparent-Namespace/delete.md %}

Por fim, iremos desmontar o diretório montado e verificar se o diretório foi removido do Alluxio 
`namespace` mas o conteúdo está preservado no armazenamento inferior:

{% include Unified-and-Transparent-Namespace/unmount.md %}

