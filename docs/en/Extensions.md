---
layout: global
group: Features
title: Under Storage Extensions
priority: 7
---

* Table of Contents
{:toc}

Alluxio can be extended with the addition of under storage modules at runtime. Under storage extensions can be built as jars and included at a specific location to be picked up by core Alluxio without the need to restart. This page describes the internals of how extensions in Alluxio work and provides instructions for building and installing an extension.

## How it Works

### Dependency Management

## Creating an Under Storage Extension

### Implementing the Under Storage Interface

### Building the Extension JAR

### Testing the Extension

## Installing the Extension
Extension JARs are picked up from the extensions directory configured using the property `alluxio.extensions.dir`. A command line utlity can be used to distribute an exension JAR to hosts running Alluxio processes. In environments where the CLI is not applicable, simply placing the JAR in the extensions directory will suffice. For example, when running in containers, a custom image can be built with extension binaries in the desired location.

### Command Line Utility

### Installing from a Maven Coordinate
