---
layout: global
title: Maven
---

# Mac OS X

Antes do OS X Mavericks, o Mac OS X possuía o Maven 3 embutido e podia ser localizado em 
`/usr/share/maven`

A partir do Mac OS X Mavericks, o Maven foi removido e necessita ser instalado manualmente.

1.  Baixar o [Maven Binary](http://maven.apache.org/download.cgi)
2.  Extrair o arquivo da distribuição, exemplo
    `apache-maven-<version.number>-bin.tar.gz`, para o diretório que você deseja instalar
    `Maven <version.number>`. Por padrão é assumido que você escolheu 
    `/System/Library/Apache-Maven`. O subdiretório `apache-maven-<version.number>`
    será criado a partir do arquivo.
3.  Em um terminal, adicione a variável de ambiente `M2_HOME`, exemplo
    `export M2_HOME=/System/Library/Apache-Maven/apache-maven-<version.number>`.
4.  Adicione a variável de ambiente `M2`, exemplo `export M2=$M2_HOME/bin`.
5.  Adicione esta variável de ambiente à variável `PATH`, exemplo
    `export PATH=$M2:$PATH`.
6.  Valide se a variável de ambiente `JAVA_HOME` está configurada para o local de instalação do 
    seu JDK, exemplo
    `export JAVA_HOME=/System/Library/Java/JavaVirtualMachines/1.6.0.jdk` e que `$JAVA_HOME/bin`
    está configurado na sua variável de ambiente `PATH`.
7.  Execute `mvn --version` para verificar que está instalado corretamente.

Alternativamente, o Maven pode ser instalado através do [Homebrew](http://brew.sh/) executando 
`brew install maven`.

# Linux

1.  Baixar o [Maven Binary](http://maven.apache.org/download.cgi)
2.  Extrair o arquivo da distribuição, exemplo
    `apache-maven-<version.number>-bin.tar.gz`, para o diretório que você deseja instalar
    `Maven <version.number>`. Por padrão é assumido que você escolheu 
    `/usr/local/apache-maven`. O subdiretório `apache-maven-<version.number>`
    será criado a partir do arquivo.
3.  Em um terminal, adicione a variável de ambiente `M2_HOME`, exemplo
    `export M2_HOME=/usr/local/apache-maven/apache-maven-<version.number>`.
4.  Adicione a variável de ambiente `M2`, exemplo `export M2=$M2_HOME/bin`.
5.  Adicione esta variável de ambiente à variável `PATH`, exemplo
    `export PATH=$M2:$PATH`.
6.  Valide se a variável de ambiente `JAVA_HOME` está configurada para o local de instalação do 
    seu JDK, exemplo `export JAVA_HOME=/usr/java/jdk1.6.0` e que `$JAVA_HOME/bin`
    está configurado na sua variável de ambiente `PATH`.
7.  Execute `mvn --version` para verificar que está instalado corretamente.

Alternativamente, o Maven pode ser instalado através de um gerenciador de pacotes 
(exemplo, `sudo apt-get install maven`).
