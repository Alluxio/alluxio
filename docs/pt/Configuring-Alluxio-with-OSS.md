---
layout: global
title: Configurando o Alluxio com OSS
nickname: Alluxio com OSS
group: Under Store
priority: 4
---

Este guia descreve como configurar o Alluxio com o [Aliyun OSS](http://www.aliyun.com/product/oss/?lang=en) 
como um sistema de armazenamento inferior. O `Object Storage Service` (OSS) é massivo, seguro e altamente 
confiável como um serviço de armazenamento na nuvem fornecido pela Aliyun.

## Configuração Inicial

Para executar o Alluxio `cluster` em um conjunto de máquina, você deve implantar os binários do Alluxio em 
cada um de seus servidores. Você pode também 
[compilar os binários a partir do código fonte](http://alluxio.org/documentation/master/Building-Alluxio-Master-Branch.html) 
ou [baixar o binário pré-compilador diretamente](http://alluxio.org/documentation/master/Running-Alluxio-Locally.html).

Então se você não ainda não criou o arquivo de configuração do `template`, faça-o:

{% include Common-Commands/copy-alluxio-env.md %}

Também, para preparar a utilização do `OSS` com o Alluxio, crie um `bucket` ou utilize um já existente. Você também deve 
tomar nota do diretório que você quer utilizar no `bucket`, ou pela criação de um novo diretório no `bucket` ou por utilizar 
um já existente. Para intenção deste guia, o nome do `bucket OSS` será chamado de `OSS_BUCKET` e o diretório será 
chamado de `OSS_DIRECTORY`. Também, para utilizar um serviço OSS, você deve prover um `endpoint OSS` para especificar 
uma variedade do `bucket`. O `endpoint` será chamado de `OSS_ENDPOINT` e para aprender mais sobre `endpoints` de uma 
variedade especial, você pode ver [aqui](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/domain-region). 
Para maiores informações sobre `OSS Bucket`, por favor, visite 
[aqui](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/function&bucket)

## Configurando o Alluxio

Para configurar o Alluxio no uso do `OSS` como um sistema de armazenamento inferior, devem ser feitas alterações no arquivo 
`conf/alluxio-env.sh`. A primeira alteração é para especificar um `OSS bucket` existente e um diretório como um sistema
de armazenamento inferior. Você pode especificar um modificando o `conf/alluxio-env.sh` para incluir:

{% include Configuring-Alluxio-with-OSS/underfs-address.md %}
    
Em seguida, você pode especificar as credenciais de acesso `OSS`. Na seção `ALLUXIO_JAVA_OPTS` do arquivo 
`conf/alluxio-env.sh`, adicione:

{% include Configuring-Alluxio-with-OSS/oss-access.md %}
    
Aqui, `<OSS_ACCESS_KEY_ID>` e `<OSS_SECRET_ACCESS_KEY>` devem ser substituídos pelas atuais 
[chaves do Aliyun](https://ak-console.aliyun.com/#/accesskey) e outras variáveis de ambiente que contém suas credenciais.
O `<OSS_ENDPOINT>` para o seu `OSS range` pode ser obtido 
[aqui](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/domain-region). 

Se você não tiver certeza de como alterar o arquivo `conf/alluxio-env.sh`, existe outra forma de prove esta configuração. 
Você pode prover o arquivo de propriedades de configuração nomeado: `alluxio-site.properties` no diretório `conf/` 
e edita-lo conforme abaixo:

{% include Configuring-Alluxio-with-OSS/properties.md %}

Depois desta alterações, o Aluuxio deve estar configurado para trabalhar com o `OSS` como `under storage system` e você 
pode rodar o Alluxio locamente com o `OSS`.

## Configurando Aplicações Distribuídas

Se você estiver utilizando um Alluxio `client` que está rodando separadamente do Alluxio `master` e dos `workers` 
(em um `JVM` separado), então você precisa ter certeza que as credenciais do Aliyun também estão fornecidas para os 
processos da aplicação `JVM`. A maneira mais fácil de fazer isso consiste em adicionar as opções na linha de comando 
quando inicializar um processo `client JVM`. Por exemplo:

{% include Configuring-Alluxio-with-OSS/java-bash.md %}

## Rodando o Alluxio Localmente com o OSS

Depois de tudo estar configurado, você pode inicializar o Alluxio localmente para ver se tudo está funcionando.

{% include Common-Commands/start-alluxio.md %}
    
Isto deve iniciar um Alluxio `master` e um Alluxio `worker`. Você pode acessar o `master UI` em http://localhost:19999.

Em seguida, você pode executar um programa de teste:

{% include Common-Commands/runTests.md %}
    
Depois de obter sucesso, você pode visitar seu diretório `OSS_BUCKET/OSS_DIRECTORY` para verificar que os arquivos e 
diretório criados pelo Alluxio existem. Para este teste, você deve ver os arquivos nomeados como:

{% include Configuring-Alluxio-with-OSS/oss-file.md %}

Para parar o Alluxio, você deve executar:

{% include Common-Commands/stop-alluxio.md %}
