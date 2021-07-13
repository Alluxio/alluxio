---
layout: global
title: Knowledge Required
group: Contribution Roadmap
priority: 10
---
# Knowledge Required (To be added)

## Git

## Java

This roadmap assumes you have some basic knowledge about Java:
* You understand Java constructs such as classes, objects, primitives, references, methods, 
inheritance, interfaces, abstract classes, and constructors.
* You can use Java commands, such as the ones listed below:
```
# Gets the version of the current java package 
$ java -version 
# Gets the running Java processes including Alluxio processes
$ jps
# Gets the stacktraces from the running processes
$ jstack -l <pid>
```
* After running a command or unit test, you can follow the code that gets executed while running within the same process. 
Some commands may involve communication across different processes that could be located on a different instance; 
these remote procedure calls or RPCs require a deeper level of understanding that is not expected of most contributors.
* You have a familiar IDE to view Java source code, run tests, and enable debugger if needed.
* You **do not** need to understand all the Java API packages, but know how to search for API descriptions when needed.
* You **do not** need to understand all the Java concepts, but know how to search for materials online to help you understand.

You do not need to be a Java expert to become an Alluxio contributor because your Java skills
will improve when you spend time reading the Alluxio source code and contributing to Alluxio.

You can expect help from the community about Alluxio specific questions, but you should be capable of 
learning about Java on your own since there are lots of online materials.

## Maven
The Alluxio project uses [Maven](https://maven.apache.org/) to manage its dependencies and to build its source code. 
Here we provide some simple commands to get you started.
For a full tutorial on maven, please consult the maven documentation.

### Build with Maven
The default command to build with maven is through the `maven install` command.
The following is a command that Alluxio developers often use to speed up the compilation. 

```console
mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Phadoop-2 -Dhadoop.version=2.7.3 -Pufs-hadoop-3 -Dufs.hadoop.version=3.3.0 -pl \!webui
```

The above command means to build using 4 cores and skip javadoc generation, skip tests, skip license header checks, skip checkstyle and findbugs checks, have alluxio export a hadoop-2 compatible interface with a hadoop version of 2.7.3, build it to support hadoop 3 as ufs with a version of 3.3.0, and finally skip the webui submodule (because it can take a while).

TODO(david/lu) add other example maven commands to run specific checks

### Dependency Analysis with Maven
Aside from building the project, Maven is also responsible for managing the dependency tree for Alluxio.
If dependencies are not managed properly, for example, if there are conflicting versions of the same package in the final jar file, it is possible to have run time failures or ClassNotFoundExceptions thrown. 

To display the dependency tree for further analysis, use the following command. 

```console
mvn dependency:tree
```

### Edit pom.xml
Sometimes it is necessary to create new pom.xml or modify an existing pom.xml as part of your feature.
In those cases, please make sure to respect the following guide.

1. Always use the root pom.xml to declare the version of the package. This ensures that the same version will be used throughout the Alluxio project. 
2. Maven provides a mechanism for packages to express [the version restrictions of their dependency.](https://maven.apache.org/pom.html#dependency-version-requirement-specification) However, this restriction is not always specified by project's pom.xml files. So if Alluxio depends on A and B and B also depends on A. It is possible that the version speicified in the root pom.xml for A is what is used instead of what is specified in B's pom.xml, causing conflict. In most cases, this conflict is benign, but it can be a source of frustrating issues.
3. If the above issue happens, we need to either adjust the alluxio dependency to the same version as B's specified version of A. Alternatively, we could shade B's included version of A. Consult shaded directory for examples of how we shade various packages to be used in Alluxio.

TODO(david/lu) add a section about maven build checks that are run by github actions

## Docker
