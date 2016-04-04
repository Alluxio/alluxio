---
layout: global
title: Contribuindo com o Alluxio
nickname: Guia do Contribuidor
group: Resources
---

* Table of Contents
{:toc}

Obrigado pelo seu interesse no Alluxio! Nós apreciamos imensamente qualquer nova funcionalidade 
ou correções de `bugs`.

### Tarefas de Iniciante do Alluxio

Existem algumas pequenas situações que os novos contribuidores podem fazer para se familiarizarem 
com o Alluxio:

1.  [Executar o Alluxio Localmente](Running-Alluxio-Locally.html)

2.  [Executar o Alluxio em um Cluster](Running-Alluxio-on-a-Cluster.html)

3.  Ler as [Definições de Configuração](Configuration-Settings.html) e a [Interface da Linha de Comando](Command-Line-Interface.html)

4.  Ler um 
    [Código Exemplo](https://github.com/alluxio/alluxio/blob/master/examples/src/main/java/alluxio/examples/BasicOperations.java)

5.  [Construir o Alluxio Master Branch](Building-Alluxio-Master-Branch.html)

6.  Efetuar o `fork` de um repositório, adicionar unidades de testes ou `javadoc` para um ou dois arquivos, e submeter uma solicitação de recebimento (`pull request`). Você também é bem-vindo para endereçar incidentes  
    (`issues`) no [JIRA](https://alluxio.atlassian.net/browse/ALLUXIO).
Aqui está a lista de 
[tarefas](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20labels%20%3D%20NewContributor%20AND%20status%20%3D%20OPEN)
para Novos Contribuidores. Por favor, limite-se em até duas tarefas classificadas como `New Contributor`.
Depois, tente tarefas como `Beginner/Intermediate` ou pergunte sobre qualquer assunto na 
[Lista de Email de Usuários](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users).
Para um tutorial, veja os guias do `GitHub` em 
[forking um repositório](https://help.github.com/articles/fork-a-repo) e
[enviando um pull request](https://help.github.com/articles/using-pull-requests).

### Submetendo o Código

-   Nós encorajamos você a dividir seu trabalho em pequenos `patches` de único propósito, se possível. 
    É muito mais difícil em fundir uma grande modificação com várias funcionalidades deslocadas.

-   Nós rastreamos incidentes e funcionalidades em nosso [JIRA](https://alluxio.atlassian.net/). Se você 
    não possui uma conta registrada, por favor, registre-se!

-   Abra um `ticket` no [JIRA](https://alluxio.atlassian.net/) detalhando a mudança proposta e para 
    o que isso serve.

-   Envie o `patch` como um `GitHub pull request`. Para um tutorial, veja os guias `GitHub` em 
    [forking um repositório](https://help.github.com/articles/fork-a-repo) e 
    [enviando um pull request](https://help.github.com/articles/using-pull-requests).

-   No título de sua solicitação de recebimento, tenha certeza de referenciar o seu `ticket JIRA`. 
    Isto irá conectar o seu `ticket` com as modificações propostas nos códigos. Por exemplo:

~~~~~
[ALLUXIO-100] Implementando uma impressionante nova funcionalidade
~~~~~

-   No campo de descrição da `pull request`, por favor, inclua um `link` para o `ticket JIRA`.


Atente que para pequenas modificações, não é necessário criar um `ticket JIRA` correspondente 
antes de enviar o `pull request`. Por exemplo:

-   Para as `pull request` que somente endereçam erros de digitação ou formatação no código fonte, 
    você pode colocar o prefixo "[SMALLFIX]" nos títulos de seus `pull requests`, por exemplo:

~~~~~
[SMALLFIX] Corrigindo a formatação no Foo.java
~~~~~

-   Para os `pull requests` que aprimoram a documentação no site do projeto do Alluxio (exemplo, 
    modifique os arquivos `markdown` no diretório `docs`), você pode colocar o prefixo "[DOCFIX]" em 
    suas `pull requests`. Por exemplo, para editar a página da `web` que é gerada a partir de 
    `docs/Contributing-to-Alluxio.md`, o título pode ser:

~~~~~
[DOCFIX] Aprimorando a documentação de como contribuir com o Alluxio
~~~~~

#### Testando

-   Execute todas as unidades de teste com ``mvn test`` (será utilizado o `file system` local como o
    `under storage system` e o `HDFS 1.0.4` como o `under storage system` com o módulo `HDFS`). O 
    comando ``mvn -Dhadoop.version=2.4.0 test`` irá utilizar o `HDFS 2.4.0` como o 
    `under storage system` para os testes com módulo `HDFS`.

-   Para executar os testes contra um específico `under storage system`,  execute o comando no 
    `maven` a partir diretório do submódulo. Por exemplo, para rodar testes para o `HDFS`, você
    deve executar ``mvn test`` a partir de ``alluxio/underfs/hdfs``.

-   Para ambientes `GlusterFS`, a unidade de teste `GlusterFS` pode ser executado a partir de 
	``alluxio/underfs/glusterfs`` com: 
	`mvn -PglusterfsTest -Dhadoop.version=2.3.0 -Dalluxio.underfs.glusterfs.mounts=/vol -Dalluxio.underfs.glusterfs.volumes=testvol test` 
	(utilize o GlusterFS como o `under filesystem`, onde o `/vol` é um `mount point GlusterFS` válido)

_   Execute uma única unidade de teste: `mvn -Dtest=AlluxioFSTest#createFileTest -DfailIfNoTests=false test`

-	Para testar, rapidamente, o funcionamento de algumas `API` de uma forma interativa, você deverá 
	elevar o `Scala shell`, como discutido neste [blog](http://scala4fun.tumblr.com/post/84791653967/interactivejavacoding).

-	Para testes com diferentes versões do `Hadoop`: ``mvn -Dhadoop.version=2.2.0 clean test``

-	Execute testes com o `Hadoop FileSystem contract tests` (utilize o `Hadoop 2.6.0`): `mvn -PcontractTest clean test`

#### Estilo de Codificação

-   Por favor, siga o estilo existente de codificação. Especialmente, nós utilizamos o 
	[estilo Google Java](http://google-styleguide.googlecode.com/svn/trunk/javaguide.html),
	com as seguintes modificações ou desvios:
	-  Tamanho máxima da linha de **100** caracteres.
	-  Importação de terceiros são agrupados em conjunto para formar uma formatação `IDE` mais simples.
	-  Nomes de variáveis de classes membros devem ter o prefixo `m`, por exemplo, 
	   `private WorkerClient mWorkerClient;`
	-  Nomes de variáveis estáticas devem ter o prefixo `s`, por exemplo 
	   `public static String sUnderFSAddress;`
-	Você pode baixar nosso [formatador Eclipse](../resources/alluxio-code-formatter-eclipse.xml)
	-  Para o Eclipse organizar as suas importações corretamente, configure o `"Organize Imports"` 
	   para parecer com [isto](../resources/eclipse_imports.png)
	-  Se você utiliza o `IntelliJ IDEA`:
	   - Você pode tanto utilizar nosso formatador com a ajuda do 
	   [Formatador de Código do Eclipse](https://github.com/krasa/EclipseCodeFormatter#instructions)
         ou utilizar o [Plugin de Formatador de Código do Eclipse](http://plugins.jetbrains.com/plugin/6546) 
         no `IntelliJ IDEA`.
       - Para formatar automaticamente a importação, configure em 
         Preferêncas->Esitlo de Código->Importação->Layout de Importação para 
         [esta ordem](../resources/intellij_imports.png)
       - Para reordenar, automaticamente, os métodos alfabeticamente, tente o 
         [Plugin Reordenar](http://plugins.jetbrains.com/plugin/173), abra Preferências, 
         pesquise por reordenar, remove comentários desnecessários, depois clique com o botão direito,
         escolha "Reordenar", então os códigos serão formatados conforme sua escolha
- 	O Alluxio está utilizando o `SLF4J` para `logs` com o típico padrão de uso:

{% include Contributing-to-Alluxio/slf4j.md %}

-  Para verificar que o código está em conformidade com o padrão, execute um 
   [checkstyle](http://checkstyle.sourceforge.net) antes de enviar um `pull request` para verificar 
   que não há novos avisos:

{% include Contributing-to-Alluxio/checkstyle.md %}

#### FindBugs

Antes de enviar seu `pull-request`, execute o 
[FindBugs](http://findbugs.sourceforge.net/) sobre o código mais recente para verificar que não 
há nenhum novo aviso.

{% include Contributing-to-Alluxio/findbugs.md %}

### IDE

Você pode gerar um arquivo de configuração do `Eclipse` através da execução:

{% include Contributing-to-Alluxio/eclipse-configuration.md %}

Então, importe o diretório para dentro do `Eclipse`.

Você também pode ter que adicionar a variável `M2_REPO` à `classpath` através da execução:

{% include Contributing-to-Alluxio/M2_REPO.md %}

Se você estiver usando o `IntelliJ IDEA`, você deve ter que alterar o perfil do `Maven` para
'desenvolvedor' a fim de evitar erros de importação. Você pode fazer isso indo para:

    Visualizar > Ferramentas de Janela > Projetos Maven

### Apresentações

-   AMPCamp 6 (Novembro, 2015)
[SlideShare](http://www.slideshare.net/TachyonNexus/tachyon-presentation-at-ampcamp-6-november-2015)
-   Strata and Hadoop World 2015 (Setembro, 2015)
[SlideShare](http://www.slideshare.net/TachyonNexus/tachyon-an-open-source-memorycentric-distributed-storage-system)
-   Strata and Hadoop World 2014 (Outubro, 2014)
[pdf](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2014-10-16-Strata.pdf)
[pptx](http://www.cs.berkeley.edu/~haoyuan/talks/Tachyon_2014-10-16-Strata.pptx)
-   Spark Summit 2014 (Julho, 2014) [pdf](http://goo.gl/DKrE4M)
-   Strata and Hadoop World 2013 (Outubro, 2013) [pdf](http://goo.gl/AHgz0E)

### Leituras

-   [Tachyon: Reliable, Memory Speed Storage for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2014_socc_tachyon.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Scott Shenker, Ion Stoica, *SOCC 2014*.
-   [Reliable, Memory Speed Storage for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2014_EECS_tachyon.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Scott Shenker, Ion Stoica, *UC Berkeley EECS 2014*.
-   [Tachyon: Memory Throughput I/O for Cluster Computing Frameworks](http://www.cs.berkeley.edu/~haoyuan/papers/2013_ladis_tachyon.pdf)
Haoyuan Li, Ali Ghodsi, Matei Zaharia, Eric Baldeschwieler, Scott Shenker, Ion Stoica, *LADIS 2013*.
