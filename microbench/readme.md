# Alluxio Microbenchmarks

This module contains microbenchmarks written in [JMH](https://github.com/openjdk/jmh) for various
Alluxio components.

## Building the benchmarks

This module is built along with others in a normal full project build. Alternatively, you can build
this module alone by running the following build command from the repository root:

```console
$ mvn -DskipTests package -pl microbench
```

This will build an uber jar named `benchmarks.jar` under `microbench/target` containing all
benchmarks in this module.

## Running the benchmarks

The generated `benchmarks.jar` is an executable jar, so you can use the following command to
run the benchmarks:

```console
$ java -jar microbench/target/benchmarks.jar <benchmark name>
```

Replace `<benchmark name>` with a benchmark name, or a regex pattern to include a set of benchmarks
whose name matches the pattern. 

## Useful options

JMH support dozens of options that let you fine tune the execution of the benchmarks and the
generation of the report. Use option `-h` to get usage and a list of options supported by JMH.

### Selecting benchmarks

Use option `-l [pattern]` to see a list of all available benchmarks. You can further specify a 
regex pattern
to list benchmarks matching the pattern:

```console
$ java -jar microbench/target/benchmarks.jar -l rpc
```

This lists all benchmarks whose fully qualified class name contains the word `rpc`.

Use `-e pattern` to exclude benchmarks from running.

### Controlling execution of benchmarks

There are plenty of options that can be used to control how a benchmark should run. Only the most 
often used ones are listed here.

Some benchmarks are parameterized. Use option `-lp` to get available parameters of the benchmarks.
Use `-p param=value` to specify a particular parameter.

Use `-i` and `-wi` to specify how many measurement and warmup iterations to run, respectively.
Use `-r` and `-w` to specify how long each measurement and warmup iteration should run for,
respectively.

Use `-f` to specify how many times to fork each benchmark. Forks are repeated runs of both the
warmup phase and the measurement phase under the same set of parameters.
Make sure to at least fork once to get more accurate results, unless you are debugging the 
benchmark from an IDE.

### Result and output

Use `-rff` to specify an output file to which the results will be saved for later data processing.
Combine with `-rf` to specify the output format (CSV or JSON, etc).

Use `-v` to enable verbose output.
