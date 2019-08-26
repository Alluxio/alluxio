package alluxio.cli.fsadmin;

import alluxio.cli.Command;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class FsadminUnitTest {
  private static Map<String, Command> sCommands;

  @Before
  public void loadFsadminCommands() {
    InstancedConfiguration cmdCommand = new InstancedConfiguration(ConfigurationUtils.defaults());
    sCommands = new FileSystemAdminShell(cmdCommand).loadCommands();
  }

  @Test
  public void checkDocs() {
    for (Map.Entry<String, Command> cmd : sCommands.entrySet()) {
      Command c = cmd.getValue();
      Assert.assertNotNull(c.getCommandName());
      Assert.assertNotNull(c.getUsage());
      Assert.assertNotNull(c.getDescription());
      Assert.assertNotNull(c.getExample());
    }
  }
}
