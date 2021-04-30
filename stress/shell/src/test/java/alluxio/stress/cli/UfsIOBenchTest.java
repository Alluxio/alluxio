package alluxio.stress.cli;

import alluxio.underfs.UfsStatus;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UfsIOBenchTest {
  @Test
  public void testPath() {
    assertEquals("/user/iotest", UfsIOBench.getDataDirPath("/user/"));
    assertEquals("/user/iotest", UfsIOBench.getDataDirPath("/user"));
    assertEquals("hdfs://user/iotest", UfsIOBench.getDataDirPath("hdfs://user/"));
    assertEquals("hdfs://user/iotest", UfsIOBench.getDataDirPath("hdfs://user"));
    assertEquals("hdfs://abc:18200/user/iotest", UfsIOBench.getDataDirPath("hdfs://abc:18200/user/"));
    assertEquals("hdfs://abc:18200/user/iotest", UfsIOBench.getDataDirPath("hdfs://abc:18200/user"));
    assertEquals("s3://user/iotest", UfsIOBench.getDataDirPath("s3://user/"));
    assertEquals("s3://user/iotest", UfsIOBench.getDataDirPath("s3://user"));
  }
}
