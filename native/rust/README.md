# Rust components in Alluxio

This module is home to Alluxio native components implemented in Rust.

## Project layout

Each Maven submodule, except for the `archetype` module, contains a Rust crate. It may also include necessary Java
wrappers for the crate.
The Rust source files are located in `src/main/rust`, and the Java wrappers are located in `src/main/java`.
After a build is complete, the build artifacts, both Java class files and Rust native binaries, are stored within
the `target/classes` directory.

## Building the modules

To build Rust sources, you will need the Rust toolchains. The recommended way is to use
[rustup](https://rust-lang.github.io/rustup/index.html), the tool for managing Rust toolchains of different versions
and for different targets. Follow the instructions to install rustup, which will then install the Rust toolchains.

The modules containing Rust crates are configured so that they can be built as part of the whole Alluxio project.
This is done by a Maven plugin that delegates the Rust build to Cargo, the Rust package manager and build system.

Running the following command from this module's root directory will build all Rust submodules:

```console
mvn clean install
```

## Generating a new submodule for Rust component

The `archetype` submodule offers a project template for new submodules containing Rust code to be used in Alluxio.
In order to use the template to generate a new submodule, you need to first install the archetype.

Run maven build for the archetype module from the root of this module:

```console
mvn clean install -f archetype
```

This will install the archetype locally so that it can be used later to generate new projects.

To generate a new submodule from the template, run the following command from the root of this module:

```console
mvn archetype:generate \
-DarchetypeGroupId=org.alluxio \
-DarchetypeArtifactId=alluxio-native-rust-archetype \
-DarchetypeVersion=305-SNAPSHOT \
-DarchetypeCatalog=local \
-DartifactId=alluxio-rust-example \
-Dcargo_crate_name=example
```

Set `archetypeVersion` to the current version of the Alluxio root project.
Set `artifactId` and `cargo_crate_name` to the name of the maven module and the Rust
crate, respectively.

This will generate a new submodule under the `native/rust` module, with the name set
by `artifactId`. The archetype generation command will also edit the pom file of the
parent module, which is `native/rust`, to add the new submodule to the module list.

> Note: The archetype plugin modifies the pom file in a way that is not compatible with
> the sortpom and license plugin. In order to pass the checks imposed by the two plugins,
> manual reverting of incompatible changes may be necessary. 

