Tachyon-Perf
============

A general performance test framework for [Tachyon](http://tachyon-project.org/).

##Run Tachyon-Perf Tests
The following steps show how to run a Tachyon-Perf test. 

1. Copy `perf/conf/tachyon-perf-env.sh.template` to `perf/conf/tachyon-perf-env.sh` and configure it as prompted.
2. Edit `perf/conf/slaves` to add testing slave nodes. It's recommended to be the same as `{tachyon.home}/conf/workers`. By the way, duplicate slave name is allowed which implies start multi-processes tests on one slave node.
3. The running test command is `perf/bin/tachyon-perf <TestCase>`
 * The parameter is the name of test case, and now it should be `Metadata`, `SimpleWrite`, `SimpleRead` or `SkipRead`.
 * The test's configurations are in `conf/testSuite/<TestCase>.xml`, and you can modify it as your wish.
4. When TachyonPerf is running, the status of the testing job will be collected and printed on the console. For some reasons, if you want to abort the tests, you can just press `Ctrl + C` to terminate it and then type the command `perf/bin/tachyon-perf-abort` on the master node to abort test processes on each slave node.
5. After all the tests finished successfully, each slave node will generate a result report, locates at `result/` by default. You can also generate a total report by the command `./bin/tachyon-perf-collect <TestCase>`.
6. In addition, command `./bin/tachyon-perf-clean` is used to clean the workspace directory on Tachyon.

##Acknowledgement
Tachyon-Perf is a project started in the Nanjing University [PASA Lab](http://pasa-bigdata.nju.edu.cn/English/index.html) and contributed to Tachyon. Any suggestions and furthure contributions are appreciated.
