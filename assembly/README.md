# Tachyon Assembly

This module is to unify the different tachyon modules into a release package; typically a tarball.

## Building

The defaults are set up so that the following command will generate the tarball

    mvn package assembly:single

This command will output the tar at `target/tachyon-$VERSION.tar.gz` along with a `target/tachyon-$VERSION` directory that has the same content as the tar.

## Contents

Inside this tar should be all the artifacts that need to be in the release: bin, conf, libexec, jars, src, ...

All content should work out of the box, so `conf` should reference the tar's structure, `bin` should execute based off this structure, and all needed jars should be included.

## Usage
For more details on how to use the generated tar, go [to the docs](http://tachyon-project.org/Running-Tachyon-Locally.html).
