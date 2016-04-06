Alluxio-Perf
============

A general performance test framework for [Alluxio](http://alluxio-project.org/).

##Compile

If you want to test Alluxio's UnderFileSystem, for example, S3UnderFileSystem, you need to add the following
dependencies to perf/pom.xml:

    <dependency>
      <groupId>org.alluxioproject</groupId>
      <artifactId>alluxio-underfs-s3</artifactId>
      <version>${project.version}</version>
      <scope>compile</scope>
    </dependency>

LocalUnderFileSystem and HdfsUnderFileSystem are already added as default.

##Run Alluxio-Perf Tests
The following steps show how to run a Alluxio-Perf test.

1. Compile and install `perf` project, run: `mvn clean install`
2. Copy `perf/conf/alluxio-perf-env.sh.template` to `perf/conf/alluxio-perf-env.sh`. Set `ALLUXIO_MASTER_HOSTNAME` and `ALLUXIO_MASTER_PORT` for alluxio master address, as well as `ALLUXIO_PERF_MASTER_HOSTNAME` and `ALLUXIO_PERF_MASTER_PORT` for the perf master.

Optional: if you are running perf against S3 UFS, please also set `ALLUXIO_UNDERFS_ADDRESS`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

3. Edit `perf/conf/slaves` to add testing slave nodes. It's recommended to be the same as `{alluxio.home}/conf/workers`. By the way, duplicate slave name is allowed which implies start multi-processes tests on one slave node.

##Other Alluxio-Perf Commands
- The running test command is `perf/bin/alluxio-perf <TestCase>`
 * The parameter is the name of test case, and now it should be `Metadata`, `SimpleWrite`, `SimpleRead`, `SkipRead`, `Mixture`, `Iterate` or `Massive`. (The detailed descriptions are shown in next section)
 * The test's configurations are in `conf/testSuite/<TestCase>.xml`, and you can modify it as your wish.
- When AlluxioPerf is running, the status of the testing job will be collected and printed on the console. For some reasons, if you want to abort the tests, you can just press `Ctrl + C` to terminate it and then type the command `perf/bin/alluxio-perf-abort` on the master node to abort test processes on each slave node.
- After all the tests finished successfully, each slave node will generate a result report, locates at `result/` by default. You can also generate a total report by the command `./bin/alluxio-perf-collect <TestCase>`.
- If any slaves failed, you can use `bin/alluxio-perf-log-collect all` to collect logs from all the slave nodes, or just the failed nodes, e.g. `bin/alluxio-perf-log-collect node1 node2`
- In addition, command `./bin/alluxio-perf-clean` is used to clean the workspace directory on Alluxio.
- A batch script `bin/alluxio-perf-batch` is also provided to run test with different xml configurations.

##Test Cases
The following shows what each of the test cases does exactly.

* **Metadata**: This test repeats performing metadata operations for a while, including creating, existing, renaming and deleting files.

* **SimpleWrite**: This test writes a set of files to the FS.

* **SimpleRead**: This test reads a set of files from the FS byte-by-byte which are written by the *SimpleWrite* test. You can configure it to read locally or remotely.

* **SkipRead**: This test is similar like the *SimpleRead* test, but it can read files in a `skip->read` pattern.

* **Mixture**: This test mixes the read and write operations in a configurable ratio. You can configure a heavy read test or a heavy write test.

* **Iterate**: This test reads/writes files iteratively that the output of the former iteration is the input of the next one. Two modes called *Shuffle* and *Non-Shuffle* are proviede. In *Shuffle* mode, each slave reads files from the whole workspace, which may lead to remote reading. In *Non-Shuffle* mode, each slave only reads the files written by itself, which keeps good locality.

* **Massive**: This test randomly chooses to read or write concurrently for a time duration. It provides *Shuffle* and *Non-Shuffle* modes as well.

##Acknowledgement
Alluxio-Perf is a project started in the Nanjing University [PASA Lab](http://pasa-bigdata.nju.edu.cn/English/index.html) and contributed to Alluxio. Any suggestions and furthure contributions are appreciated.
