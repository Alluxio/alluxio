---
layout: global
title: Configurando Alluxio com Swift
nickname: Alluxio com Swift
group: Under Store
priority: 1
---

Este guia descreve como configurar o Alluxio com 
[Swift](http://docs.openstack.org/developer/swift/) sendo o armazenamento inferior.

# Configuração Inicial

Primeiro, os binários do Alluxio devem estar em sua máquina. Você pode 
[compilar o Alluxio](Building-Alluxio-Master-Branch.html) ou
[baixar os binários localmente](Running-Alluxio-Locally.html).

Depois, crie o arquivo de configuração a partir do `template`, caso ainda não tenha efetuado:

{% include Common-Commands/copy-alluxio-env.md %}

# Configurando o Alluxio

Para configurar o Alluxio com o `Swift` como armazenamento inferior, devem ser efetuadas 
alterações no arquivo `conf/alluxio-env.sh`. A primeira alteração é em especificar o endereço 
do sistema de armazenamento inferior. Você especifica isto incluindo no `conf/alluxio-env.sh` 
a alteração:

{% include Configuring-Alluxio-with-Swift/underfs-address.md %}

Onde `<swift-container>` é o `Swift container` existente.

A configuração seguinte deve ser informado dentro do `conf/alluxio-env.sh`

{% include Configuring-Alluxio-with-Swift/several-configurations.md %}
  	
Os possíveis valores de `<swift-use-public>` são `true`, `false`.
Os possíveis valores de `<swift-auth-model>` são `keystone`, `tempauth`, `swiftauth`

No sucesso da autenticação, `Keystone` irá retornar dois acessos `URLs`: `public` e `private`. 
Se o Alluxio estiver sendo usado dentro da rede da sua companhia e o `Swift` está localizado 
na mesma rede, é recomendado em definir o valor de `<swift-use-public>`  para `false`.

## Acessando o IBM SoftLayer object store

Utilizando o módulo `Swift` também faz com que o `IBM SoftLayer object store` seja uma opção de 
`under storage system` para o Alluxio. O `SoftLayer` requer que o `<swift-auth-model>`
esteja configurado como `swiftauth`

# Rodando o Alluxio Localmente com Swift

Depois que tudo estiver configurado, você pode inicializar o Alluxio localmente para ver se tudo 
está funcionando.

{% include Common-Commands/start-alluxio.md %}

Isso deve inicializar um Alluxio `master` e um Alluxio `worker`. Você pode ver o `master UI` em 
[http://localhost:19999](http://localhost:19999).

A seguir, você pode executar um simples programa de teste:

{% include Common-Commands/runTests.md %}

Se você obter sucesso, você pode visitar o seu `Swift container` para verificar se os arquivos e 
diretórios criados pelo Alluxio existem. Para este teste, você deve ser arquivos nomeados como:

{% include Configuring-Alluxio-with-Swift/swift-files.md %}

Para parar o Alluxio, você pode executar:

{% include Common-Commands/stop-alluxio.md %}

# Executando testes funcionais com o IBM SoftLayer

Configure sua conta `Swift` ou `SoftLayer` dentro do `testes/pom.xml`, onde `authMethodKey` 
deve ser `keystone`, `tempauth` ou `swiftauth`. Para executar os testes funcionais:

{% include Configuring-Alluxio-with-Swift/functional-tests.md %}

Em caso de falhas, os `logs` estarão localizados em `tests/target/logs`. Você também 
deve ativar o `heap dump` através de:

{% include Configuring-Alluxio-with-Swift/heap-dump.md %}
