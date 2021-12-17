package alluxio.server.worker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;

import net.bytebuddy.utility.RandomString;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class WorkerLoggingIntegrationTest {

  /**
   * This test makes ensures log4j does not hold onto old logging files that have been deleted.
   */
  @Test
  public void workerLog4jStressTest() throws Exception {
    MultiProcessCluster mCluster =
        MultiProcessCluster.newBuilder(PortCoordination.WORKER_LOG4J_STRESS)
            .setClusterName("workerLog4jStressTest")
            .setNumMasters(1)
            .setNumWorkers(2)
            .addLog4jProperty("log4j.rootLogger", "DEBUG, ${alluxio.logger.type}, ${alluxio.remote"
                + ".logger.type}")
            // changing the name from the default log file name to avoid collisions
            .addLog4jProperty("log4j.appender.WORKER_LOGGER.File", "${alluxio.logs.dir}/worker"
                + ".testlog")
            .addLog4jProperty("log4j.appender.WORKER_LOGGER.MaxFileSize", "1MB")
            .addLog4jProperty("log4j.appender.WORKER_LOGGER.MaxBackupIndex", "2")
            .build();
    mCluster.start();

    for (int i = 0; i < 100; i++) {
      AlluxioURI path = new AlluxioURI("/" + RandomString.make());
      mCluster.getFileSystemClient().createDirectory(path);
      boolean exists = mCluster.getFileSystemClient().exists(path);
      assertTrue(exists);
      mCluster.getFileSystemClient().delete(path);
      exists = mCluster.getFileSystemClient().exists(path);
      assertFalse(exists);
    }

    ProcessBuilder builder = new ProcessBuilder("sh", "-c", "lsof -c java | grep \"worker"
        + ".testlog\"");
    builder.inheritIO().redirectOutput(ProcessBuilder.Redirect.PIPE);
    Process process = builder.start();
    // wait for lsof to complete
    process.waitFor();
    String out;
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(process.getInputStream()))) {
      // iterates over all open files with "worker.testlog" as a prefix
      while ((out = reader.readLine()) != null) {
        if (out.contains("(deleted)")) {
          // log4j is holding onto deleted files and the test must fail
          Assert.fail();
        }
      }
    }
    mCluster.notifySuccess();
  }
}
