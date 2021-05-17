---
layout: global
title: Knowledge Required
group: Contribution Roadmap
priority: 10
---
# Knowledge Required (To be added)

## Git

## Java

## Maven
The Alluxio project uses [Maven](https://maven.apache.org/) to manage its dependencies and to build its source code. 
Here we provide some simple commands to get you started.
For a full tutorial on maven, please consult the maven documentation.

### Build with Maven
The default command to build with maven is through the `maven install` command.
The following is a command that Alluxio developers often use to speed up the compilation. 

```bash
mvn -T 4C clean  install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true  -Phadoop-2 -Dhadoop.version=2.7.3 -Pufs-hadoop-3  -Dufs.hadoop.version=3.3.0 -pl \!webui
```

The above command means to build using 4 cores and skip javadoc generation, skip tests, skip license header checks, skip checkstyle and findbugs checks, have alluxio export a hadoop-2 compatible interface with a hadoop version of 2.7.3, build it to support hadoop 3 as ufs with a version of 3.3.0, and finally skip the webui submodule (because it can take a while).

### Dependency Analysis with Maven
Aside from building the project, Maven is also responsible for managing the dependency tree for Alluxio.
If dependencies are not managed properly, for example, if there are conflicting versions of the same package in the final jar file, it is possible to have run time failures or ClassNotFoundExceptions thrown. 

To display the dependency tree for further analysis, use the following command. 

```bash
mvn dependency:tree
```

### Edit pom.xml
Sometimes it is necessary to create new pom.xml or modify an existing pom.xml as part of your feature.
In those cases, please make sure to respect the following guide.

1. Always use the root pom.xml to declare the version of the package. This ensures that the same version will be used throughout the Alluxio project. 
2. Maven provides a mechanism for packages to express [the version restrictions of their dependency.](https://maven.apache.org/pom.html#dependency-version-requirement-specification) However, this restriction is not always specified by project's pom.xml files. So if Alluxio depends on A and B and B also depends on A. It is possible that the version speicified in the root pom.xml for A is what is used instead of what is specified in B's pom.xml, causing conflict. In most cases, this conflict is benign, but it can be a source of frustrating issues.
3. If the above issue happens, we need to either adjust the alluxio dependency to the same version as B's specified version of A. Alternatively, we could shade B's included version of A. Consult shaded directory for examples of how we shade various packages to be used in Alluxio.

## Docker
