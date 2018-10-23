---
layout: global
title: Segurança
group: Features
priority: 1
---

* Table of Contents
{:toc}

Atualmente, existem duas funcionalidades para a segurança do Alluxio. Este documento menciona
estes conceitos e suas utilizações.

1. [Autenticação](#autenticação): Se habilitado, o `file system` do Alluxio pode reconhecer e
verificar o usuário que está solicitando o acesso a este. Esta funcionalidade é a base para
outras funcionalidades de segurança como autorização e criptografia.
2. [Autorização](#autorização): Se habilitado, o `file system` do Alluxio pode controlar o
acesso do usuário. O modelo de autorização `POSIX` é utilizado no Alluxio para assinalar
permissões e direitos de controle de acesso.

Por padrão o Alluxio executa sem modo de segurança onde nem autenticação e nem autorização são
necessários. Veja as
[configurações-de-segurança](Configuration-Settings.html#configurações-de-segurança) para
habilitar e utilizar as funcionalidades de segurança.

## Autenticação

O Alluxio provê o serviçco de `file system` através do `Thrift RPC`.  O lado do `client`
(representado pelo usuário) e o lado do servidor (como o `master`) devem estabelecer uma conexão
autenticada para comunicação. Se a autenticação tiver sucesso, a conexão será estabelecida.
Se houver falhar, a conexão não será estabelecida e uma exceção será direcionada para o `client`.

Existem três tipos de autenticação suportados: `NOSASL` (modo padrão); `SIMPLE`; e `CUSTOM`.

### Contas de Usuários

As entidades de comunicação no Alluxio consistem de o `master`, o `worker` e o `client`. Cada um
destes necessita saber qual o usuário que está executando, também chamado de usuário de `login`.
O `JAAS (Java Authentication and Authorization Service)` é utilizado para determinar quem é que
está executando o processo, atualmente.

Quando a autenticação está habilitada, o usuário de `login` para o componente (`master`, `worker`
ou `client`) pode obter os seguintes passos:

1. `Login` por usuário configurável. Se a propriedade `'alluxio.security.login.username'` está
definida pela aplicação, este valor será o usuário de `login`.
2. Se o valor estiver vazio, o `login` será o usuário do `SO`.

Se o `login` falhar, uma exceção será informada. Se houver sucesso:

1. Para o `master`, o usuário de `login` é o super usuário para o `file system` do Alluxio. Este
também será o proprietário do diretório raiz.
2. Para o `worker` e o `client`, o usuário de `login` é o usuário que efetua o contato com o
`master` para acessar o arquivo. Este é informado para o `master` através de uma conexão `RPC`
para autenticação.

### NOSASL

Neste modo a autenticação é desabilitada. O `file system` do Alluxio se comporta como anteriormente.
`SASL (Simple Authentication and Security Layer)` é um estrutura para definir a autenticação
entre aplicações cliente e servidor, que é utilizado no Alluxio para implantar a funcionalidade
de autenticação. Então o `NOSASL` é usado para desabilitar esta funcionalidade.

### SIMPLE

Neste modo a autenticação é habilitada. O `file system` do Alluxio tem a informação do usuário
que está acessando e este entende que o usuário possui o privilégio.

Depois que o usuário criar diretórios ou arquivos, o nome do usuário é adicionado ao metadados.
A informação deste usuário pode ser lidar e visualizada pelo `CLI` e `UI`.

### CUSTOM

Neste modo a autenticação é habilitada. O `file system` do Alluxio tem a informação do usuário
que está acessando e utiliza o `AuthenticationProvider` para verificar se o usuário possui ou
não privilégio.

Experimental. Este modo é utilizado apenas para testes, atualmente.

## Autorização

O `file system` do Alluxio implementa o modelo de permissão ara diretórios e arquivos que é
similar ao modelo `POSIX`.

Cada arquivo e diretório está associado com:

1. um proprietário, que é o usuário do processo `client` para criar o arquivo ou diretório.
2. um grupo, que é o grupo retornado pelo serviço `user-groups-mapping`. Veja o
[Mapeamento de Grupo por Usuário](#mapeamento-de-grupo-por-usuário).
3. permissões

As permissões possuem três partes:

1. a permissão de proprietário define o acesso privilegiado ao proprietário do arquivo
2. a permissão de grupo define o acesso privilegiado ao grupo proprietário
3. a permissão de outros define o acesso privilegiado para todos os usuários que não pertencem
a nenhuma das duas classes acima

Cada permissão possui três ações:

1. leitura (r)
2. escrita (w)
3. execução (x)

Para os arquivos, a permissão `r` é necessário para leitura e a `w` é necessário para escrever o
arquivo. Para os diretórios, a permissão `r` é requerida para listar o conteúdo do diretórios, a
`w` para criar, renomear ou apagar arquivos e diretórios abaixo deste, e a permissão `x` é
requerida para acesso ao diretório filho.

Por exemplo, quando a autorização está habilitada, o resultado do comando `ls -R` é:

{% include Security/lsr.md %}

### Mapeamento de Grupo por Usuário

Quando o usuário é definido, a lista de grupos é definida por um serviço de mapeamento de grupo,
configurado por `alluxio.security.group.mapping.class`. A implementação padrão é o
`'alluxio.security.group.provider.ShellBasedUnixGroupsMapping'`, que executa o comando `groups`
no terminal para retornar o grupo de um usuário informado.

A propriedade `'alluxio.security.authorization.permission.supergroup'` define um super grupo.
Qualquer usuário pertencente a este grupo também serão super usuários.

### Permissões Inicializadas de Diretórios e Arquivos

A permissão inicial de criação é 777 e a diferença entre um diretório e um arquivo é 111. Por
padrão o valor de `umask` é 022, o diretório criado possui permissão 755 e o arquivo possui a 644.
O valor de `umask` pode ser definido através da propriedade
`'alluxio.security.authorization.permission.umask'`.

### Atualização do Modelo de Permissão para o Diretório e o Arquivo

O proprietário, grupo e as permissões podem ser alterados de duas maneiras:

1. O usuário de aplicação invoca o método `setAttribute(...)` da `FileSystem API` ou `Hadoop API`.
Veja o [File System API](File-System-API.html).
2. Comandos `CLI` no terminal. Veja os comandos
[chown, chgrp, chmod](Command-Line-Interface.html#lista-de-operações).

O proprietário somente pode ser modificado por um super usuário.
O grupo e a permissão somente podem ser modificados por um super usuário e o proprietário do arquivo.

## Desenvolvimento

É recomendado para iniciar o Alluxio `master` e os `workers` através do mesmo usuário. O serviço
do Alluxio `cluster` é composto de um `master` e os `workers`. Cada `worker` precisa efetuar uma
chamada `RPC` para o `master` por algumas operações de arquivos. Se o usuário do `worker` não
for o mesmo do `master`, a operação deste arquivo irá falhar devido a checagem n permissão.
