package alluxio.client.cli.fsadmin.command;

import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;

import org.junit.Assert;
import org.junit.Test;
import java.io.FileNotFoundException;

public class NodesCommandIntegrationTest extends AbstractFsAdminShellTest {
  /* This test relies on the support of running master-less
   * alluxio mini cluster to start with for StaticMembershipManager
   * and EtcdMembershipManager. Currrently only add basic tests.
   */
  
  @Test
  public void testNoopMemberManager() throws FileNotFoundException {
    int ret = mFsAdminShell.run("nodes", "status");
    Assert.assertEquals(0, ret);
  }
}
