---
layout: global
title: Configurando o Alluxio com Amazon S3
nickname: Alluxio com S3
group: Under Store
priority: 0
---

Este guia descreve como configurar o Alluxio com [Amazon S3](https://aws.amazon.com/s3/) 
como um sistema de armazenamento inferior.

# Configuração Inicial

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

# Configurando o Alluxio

Para configurar o Alluxio para utilizar o `S3` como um sistema de armazenamento inferior, 
modificações no arquivo `conf/alluxio-env.sh` devem ser efetuados. A primeira modificação é 
para especificar um `S3 bucket` **existente** e um diretório como um `under storage system`. 
Você pode especificar isso modificando o arquivo `conf/alluxio-env.sh` para incluir:

{% include Configuring-Alluxio-with-S3/underfs-address.md %}

A seguir, você precisa especificar as credenciais `AWS` para o acesso do `S3`. Na seção  
`ALLUXIO_JAVA_OPTS` do arquivo `conf/alluxio-env.sh`, adicione:

{% include Configuring-Alluxio-with-S3/aws.md %}

Aqui, o `<AWS_ACCESS_KEY_ID>` e `<AWS_SECRET_ACCESS_KEY>` devem ser substituídos pelas suas atuais 
[chaves de segurança AWS](https://aws.amazon.com/developers/access-keys) ou outras variáveis de 
ambiente que contenham suas credenciais.

Depois dessas alterações, o Alluxio deve ser configurado para trabalhar com o `S3` como o 
sistema de armazenamento inferior e você pode tentar 
[Executar o Alluxio localmente com S3](#executando-o-alluxio-localmente-com-s3).

## Acessando S3 através de um Proxy

Para comunicar com o `S3` através de um `proxy`, modifique a seção `ALLUXIO_JAVA_OPTS` do arquivo 
`conf/alluxio-env.sh` para incluir:

{% include Configuring-Alluxio-with-S3/proxy.md %}

Aqui, o `<PROXY_HOST>` e `<PROXY_PORT>` deve ser substituído pelo servidor e porta do seu `proxy` e 
o `<USE_HTTPS?>` deve ser definido para `true` ou `false`, dependendo se o seu `proxy` utiliza 
comunicação `HTTPS`.

Estes parâmetros de configuração também devem ser definidos para o Alluxio `client` se este 
estiver rodando em um `JVM` apartado do Alluxio `Master` e dos `Workers`. Veja 
[Configurando Aplicações Distrubuídas](#configurando-aplicações-distríbuidas)

# Configurando Sua Aplicação

Quando estiver montando sua aplicação para utilizar o Alluxio, sua aplicação deverá ter que incluir 
o módulo `alluxio-core-client`. Se você estiver utilizando o [maven](https://maven.apache.org/), 
você pode adicionar a dependência para sua aplicação com:

{% include Configuring-Alluxio-with-S3/dependency.md %}

## Habilitando o Hadoop S3 Client (ao invés do S3 client nativo)

O Alluxio provê um `client` nativo para se comunicar com `S3`. Por padrão, o `S3 client` nativo é 
utilizado quando o Alluxio está configurado para usar o `S3` como um sistema de armazenamento 
inferior.

Entretanto, existe também uma opção para utilizar uma implementação diferente para se comunicar com 
o `S3`. O `S3 client` fornecido pelo `Hadoop`. Para desabilitar o Alluxio `S3 client` (e habilitar 
o `Hadoop S3 client`), modificações adicionais devem ser feitas para a sua aplicação. Quando 
incluir o módulo `alluxio-core-client` na sua aplicação, o `alluxio-underfs-s3` deve ser excluído 
para desabilitar o `client` nativo e utilizar o `Hadoop S3 client`:

{% include Configuring-Alluxio-with-S3/hadoop-s3-dependency.md %}

Contudo, o `Hadoop S3 client` precisa do pacote `jets3t` para utilizar o `S3` mas este não está 
incluído como uma dependência automaticamente. Sendo assim, você precisará adicionar a 
dependência manualmente. Quando utilizar o `maven`, você pode adicionar os passos a seguir para 
introduzir a dependência do `jets3t`:

{% include Configuring-Alluxio-with-S3/jets3t-dependency.md %}

A versão `jets3t 0.9.0` trabalha com a versão `Hadoop 2.3.0`. A versão `jets3t 0.7.1` deve funcionar 
com as versões anteriores do `Hadoop`. Para procurar a versão exato do `jets3t` para a sua versão 
do `Hadoop`, por favor, pesquise no [MvnRepository](http://mvnrepository.com/).

## Configurando Aplicações Distribuídas

Se você estiver usando um Alluxio `client` que roda apartado do Alluxio `Master` e dos `Workers` 
em um `JVM` separado), então você precisa ter certeza de que as suas credenciais `AWS` também 
estão disponíveis para os processos da aplicação `JVM`. A maneira mais fácil de fazer isso é 
adicionando estes como opções de linha de comando quando iniciar o processo do seu `client JVM`. 
Por exemplo:

{% include Configuring-Alluxio-with-S3/java-bash.md %}

# Executando o Alluxio Localmente com S3

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
