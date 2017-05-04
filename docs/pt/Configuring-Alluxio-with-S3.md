---
layout: global
title: Configurando o Alluxio com Amazon S3
nickname: Alluxio com S3
group: Under Store
priority: 0
---

* Table of Contents
{:toc}

Este guia descreve como configurar o Alluxio com [Amazon S3](https://aws.amazon.com/s3/)
como um sistema de armazenamento inferior.

## Configuração Inicial

Primeiro, os binários do Alluxio devem estar na sua máquina. Você pode
[compilar o Alluxio](Building-Alluxio-Master-Branch.html) ou
[baixar os binários localmente](Running-Alluxio-Locally.html).

Depois, se você ainda não tiver efetuado, crie o arquivo de configuração a partir do modelo:

{% include Common-Commands/copy-alluxio-env.md %}

Em preparação para utilizar o `S3` com o Alluxio, crie um `bucket` (ou utilize um `bucket`
existente). Você também pode anotar o diretório que deseja utilizar neste `bucket`, ou por
criar um novo diretório neste `bucket` ou por escolher um já existente. Pela intenção deste
guia, o nome do `S3 bucket` será chamado de `S3_BUCKET` e o diretório neste `bucket` será
chamado de `S3_DIRECTORY`.

## Configurando o Alluxio

Para configurar o Alluxio para utilizar o `S3` como um sistema de armazenamento inferior,
modificações no arquivo `conf/alluxio-env.sh` devem ser efetuados. A primeira modificação é
para especificar um `S3 bucket` **existente** e um diretório como um `under storage system`.
Você pode especificar isso modificando o arquivo `conf/alluxio-env.sh` para incluir:

{% include Configuring-Alluxio-with-S3/underfs-address-s3n.md %}

A seguir, você precisa especificar as credenciais `AWS` para o acesso do `S3`. Na seção
`ALLUXIO_JAVA_OPTS` do arquivo `conf/alluxio-env.sh`, adicione:

{% include Configuring-Alluxio-with-S3/aws.md %}

Aqui, o `<AWS_ACCESS_KEY_ID>` e `<AWS_SECRET_ACCESS_KEY>` devem ser substituídos pelas suas atuais
[chaves de segurança AWS](https://aws.amazon.com/developers/access-keys) ou outras variáveis de
ambiente que contenham suas credenciais.

Depois dessas alterações, o Alluxio deve ser configurado para trabalhar com o `S3` como o
sistema de armazenamento inferior e você pode tentar
[Executar o Alluxio localmente com S3](#executando-o-alluxio-localmente-com-s3).

### Acessando S3 através de um Proxy

Para comunicar com o `S3` através de um `proxy`, modifique a seção `ALLUXIO_JAVA_OPTS` do arquivo
`conf/alluxio-env.sh` para incluir:

{% include Configuring-Alluxio-with-S3/proxy.md %}

Aqui, o `<PROXY_HOST>` e `<PROXY_PORT>` deve ser substituído pelo servidor e porta do seu `proxy` e
o `<USE_HTTPS?>` deve ser definido para `true` ou `false`, dependendo se o seu `proxy` utiliza
comunicação `HTTPS`.

## Configurando Sua Aplicação

Quando estiver montando sua aplicação para utilizar o Alluxio, sua aplicação deverá ter que incluir
o módulo `alluxio-core-client-fs`. Se você estiver utilizando o [maven](https://maven.apache.org/),
você pode adicionar a dependência para sua aplicação com:

{% include Configuring-Alluxio-with-S3/dependency.md %}

## Executando o Alluxio Localmente com S3

Depois que tudo estiver configurado, você pode iniciar o Alluxio localmente para ver se tudo
funciona.

{% include Common-Commands/start-alluxio.md %}

Isto deve iniciar um Alluxio `master` e um Alluxio `worker`. Você pode ver a
interface de usuário do `master` em [http://localhost:19999](http://localhost:19999).

Em seguida, você pode rodar um simples programa de teste:

{% include Common-Commands/runTests.md %}

Depois que obter sucesso neste teste, você pode acessar o seu diretório `S3` em
`S3_BUCKET/S3_DIRECTORY` para verificar se os arquivos e diretórios criados pelo Alluxio
existem. Para este teste, você deve ver arquivos nomeados como:

{% include Configuring-Alluxio-with-S3/s3-file.md %}

Para parar o Alluxio, você pode executar:

{% include Common-Commands/stop-alluxio.md %}
