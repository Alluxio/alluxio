Tachyon-Perf
============

A general performance test framework for [Tachyon](http://tachyon-project.org/).

##Run Tachyon-Perf Tests
Following is the basic steps to run a Tachyon-Perf test. 

1. Copy `conf/tachyon-perf-env.sh.template` to `conf/tachyon-perf-env.sh` and configure it.
2. Edit `conf/slaves` to add slave nodes. It's recommended to be the same as `{tachyon.home}/conf/slaves`. By the way, replicated slave name is allowed which means multi-processes on one slave node.
3. The running command is `./bin/tachyon-perf <TaskType>`
 * The parameter is the type of test task, and now it should be `Metadata`, `SimpleWrite`, `SimpleRead` or `SkipRead`.
 * The task's configurations are in `conf/testSuite/<TaskType>.xml`, and you can modify it as your wish.
4. When TachyonPerf is running, the status of the test job will be printed on the console. For some reasons, if you want to abort the tests, you can just press `Ctrl + C` to terminate it and then type the command `./bin/tachyon-perf-abort` at the master node to abort test processes on each slave node.
5. After all the tests finished successfully, each node will generate a result report, locates at `result/` by default. You can also generate a total report by the command `./bin/tachyon-perf-collect <TaskType>`.
6. In addition, command `./bin/tachyon-perf-clean` is used to clean the workspace directory on Tachyon.

