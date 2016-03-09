---
layout: global
title: Configurando o Alluxio com GlusterFS
nickname: Alluxio com GlusterFS
group: Under Store
priority: 2
---

Este guia descreve como configurar o Alluxio com o [GlusterFS](http://www.gluster.org/) 
como o sistema de armazenamento inferior.

# Configuração Inicial

Primeiro, os binários do Alluxio devem estar na sua máquina. Você pode 
[compilar o Alluxio](Building-Alluxio-Master-Branch.html) ou
[baixar os binários localmente](Running-Alluxio-Locally.html).

Depois, se você ainda não tiver efetuado, crie o arquivo de configuração a partir do modelo:

{% include Common-Commands/copy-alluxio-env.md %}

# Configurando o Alluxio

Assumindo que o `GlusterFS` está alocado nos nós do Alluxio, o volume do `GlusterFS` é 
montado em `/alluxio_vol`, a variável de ambiente precisa ser adicionada em 
`conf/alluxio-env.sh`:

{% include Configuring-Alluxio-with-GlusterFS/underfs-address.md %}

# Rodando o Alluxio Localmente com o GlusterFS

Depois que tudo estiver configurado, você pode iniciar o Alluxio localmente para ser se está 
tudo funcionando.

{% include Common-Commands/start-alluxio.md %}

Isto deve iniciar um Alluxio `master` e um Alluxio `worker`. Você pode ver a 
interface de usuário do `master` em [http://localhost:19999](http://localhost:19999).

Em seguida, você pode rodar um simples programa de teste:

{% include Common-Commands/runTests.md %}

Depois que obter sucesso neste teste, você pode acessar o seu volume `GlusterFS` para verificar se os  
arquivos e diretórios criados pelo Alluxio existem. Para este teste, você deve ver arquivos 
nomeados como:

{% include Configuring-Alluxio-with-GlusterFS/glusterfs-file.md %}

Para parar o Alluxio, você pode executar:

{% include Common-Commands/stop-alluxio.md %}
