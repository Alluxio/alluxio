package alluxio;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AlluxioProcess}.
 */
public class AlluxioProcessTest {
  @Test
  public void typeTest() {
    AlluxioProcess.setType(AlluxioProcess.Type.CLIENT);
    Assert.assertEquals(AlluxioProcess.Type.CLIENT, AlluxioProcess.getType());
    AlluxioProcess.setType(AlluxioProcess.Type.MASTER);
    Assert.assertEquals(AlluxioProcess.Type.MASTER, AlluxioProcess.getType());
    AlluxioProcess.setType(AlluxioProcess.Type.WORKER);
    Assert.assertEquals(AlluxioProcess.Type.WORKER, AlluxioProcess.getType());
  }
}
