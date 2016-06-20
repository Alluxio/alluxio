package alluxio.client;

import alluxio.AlluxioProcess;
import alluxio.Context;
import alluxio.master.MasterContext;
import alluxio.worker.WorkerContext;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link alluxio.Context}.
 */
public class ContextTest {
  @Test
  public void clientContextTest() {
    AlluxioProcess.setType(AlluxioProcess.Type.CLIENT);
    ClientContext.getConf().set("test_key", "client");
    Assert.assertEquals(Context.getConf().get("test_key"), "client");
  }

  @Test
  public void masterContextTest() {
    AlluxioProcess.setType(AlluxioProcess.Type.MASTER);
    MasterContext.getConf().set("test_key", "master");
    Assert.assertEquals(Context.getConf().get("test_key"), "master");
  }

  @Test
  public void workerContextTest() {
    AlluxioProcess.setType(AlluxioProcess.Type.WORKER);
    WorkerContext.getConf().set("test_key", "worker");
    Assert.assertEquals(Context.getConf().get("test_key"), "worker");
  }
}
