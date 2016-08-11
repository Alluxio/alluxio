---
layout: global
title: Thrift
---

# Mac OS X

Antes que você instale o [Apache Thift](http://thrift.apache.org), primeiro, você precisará  
configurar o suporte a linha de comando. Para fazer isso, você irá:

Instalar o Xcode a partir do Mac App Store

Inicie o Xcode, abra a janela `Preferences`, selecione `Downloads` e instale
    o componente “Command Line Tools for Xcode”.

## Homebrew

Esta seção explica como instalar o Apache Thrift através do [Homebrew](http://brew.sh/).

Primeiro, instale o [Homebrew](http://brew.sh/).

Aqui estão os comandos para a instalação do `Homebrew`:

{% include Thrift/install-Homebrew.md %}

Utilize o `Homebrew` para instalar o `autoconf`, `automake`, `libtool`e `pkg-config`:

{% include Thrift/install-dependency-brew.md %}

Utilize o `Homebrew` para instalar o [Boost](http://www.boost.org/)

{% include Thrift/install-Boost-brew.md %}

Instale o `Thrift`

{% include Thrift/install-Thrift-brew.md %}

## MacPorts

Esta seção explica como instalar o Apache Thrift através do [MacPorts](http://macports.org).

Se você utiliza o [MacPorts](http://macports.org), as instruções a seguir irão te ajudar.

Instale o MacPorts a partir do site [sourceforge](http://sourceforge.net/projects/macports/).

Atualize o `Port`:

{% include Thrift/update-port.md %}

Utilize o `Port` para instalar o `flex`, `bison`, `autoconf`, `automake`, `libtool` e `pkgconfig`:

{% include Thrift/install-dependency-port.md %}

Utilize o `Port` para instalar o [Boost](http://www.boost.org/)

{% include Thrift/install-Boost-port.md %}

Tente utilizar o `Port` para instalar o `Thrift`:

{% include Thrift/install-Thrift-port.md %}

O último comando PODE falhar, de acordo com o [problema](https://trac.macports.org/ticket/41172). 
Neste caso, nós recomendamos construir o `Thrift 0.9.2` a partir do código fonte (Levando em consideração
que você usa o diretório padrão do `MacPort` em `/opt/local`):

{% include Thrift/build-Thrift-port.md %}

Você pode modificar o `CXXFLAGS`. Aqui nós incluímos `/usr/include/4.2.1` para `std::tr1` no Mavericks e
`/opt/local/lib` para bibliotecas instaladas pelo `Port`. Sem o `-I`, a instalação pode falhar com 
`tr1/functional not found`. Sem o `-L`, a instalação pode falhar durante o `linking`.

# Linux

[Referência](http://thrift.apache.org/docs/install/)

## Debian/Ubuntu

O comando a seguir instala todas as ferramentas e bibliotecas necessárias para instalar o  
compilador Apache Thrift em um sistema Debian/Ubuntu Linux

{% include Thrift/install-dependency-apt.md %}

ou

{% include Thrift/install-dependency-yum.md %}

Depois, instale o Java JDK de sua escolha. Digite javac para ver a lista de pacotes disponíveis,
escolha o de sua preferência e utilize o gerenciador de pacotes para o instalar.

Os usuários Debian Lenny Users podem precisar de alguns pacotes do `backports`:

{% include Thrift/install-lenny-backports.md %}

[Construir o Thrift](http://thrift.apache.org/docs/BuildingFromSource):

{% include Thrift/build-Thrift-ubuntu.md %}

## CentOS

Os passos a seguir podem ser utilizados para configurar um sistema CentOS 6.4.

Instale as dependências:

{% include Thrift/install-dependency-centos.md %}

Atualize o `autoconf` para a versão 2.69 (`yum` provavelmente irá baixar a versão 2.63 que não funciona com o Apache Thrift):

{% include Thrift/update-autoconf.md %}

Baixe e instale o código fonte do Apache Thrift:

{% include Thrift/download-install-Thrift.md %}

# Gerando os arquivos Java files do Thrift

Alluxio define um serviço RPC utilizando o arquivo `thrift` localizado em:

    ./common/src/thrift/alluxio.thrift

e gera os arquivos Java deste dentro de:

    ./common/src/main/java/alluxio/thrift/

Para gerar novamente os arquivos java se o arquivo `thrift` foi modificado, você pode executar:

{% include Thrift/regenerate.md %}
