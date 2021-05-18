---
layout: global
title: Knowledge Required
group: Contribution Roadmap
priority: 10
---
# Knowledge Required (To be added)

## Git

## Java

This roadmap assumes you have the basic knowledge about Java:
* You understand the Java basics e.g. classes, objects, primitives, references, methods, 
inheritance, interfaces, abstract classes, constructors.
* You can use the basic Java commands. Part of the most important commands are listed below:
```
# Gets the version of the current java package 
$ java -version 
# Gets the running Java processes including Alluxio processes
$ jps
# Gets the stacktraces from the running processes
$ jstack -l <pid>
```
* You can follow an operation from end to end in source code if no RPC framework is involved
(RPC framework adds some complexities, but the
[Getting started with Alluxio contribution docs]({{ '/en/contribution-roadmap/Getting-Started-With-Alluxio-Contribution.html' | relativize_url }})
will help you get through it). 
This can be achieved by manually following the code or by running operations with
[remote debugger]({{ '/en/operation/Troubleshooting.html' | relativize_url }}#alluxio-remote-debug)
or by running unit tests in debug mode via IDEs.
* You have a familiar IDE to view Java source codes, run tests, and enable debugger if needed.
* You **do not** need to understand all the Java API packages, but know how to search for API descriptions when needed.
* You **do not** need to understand all the Java concepts, but know how to search for materials online to help you understand.

You do not need to be a Java expert to become an Alluxio contributor because your Java skills
will improve when you spend time reading the Alluxio source code and contributing to Alluxio.

You can expect help from the community about Alluxio specific questions, but you should be capable of 
learning about Java on your own since there are lots of online materials.

## Maven

## Docker
