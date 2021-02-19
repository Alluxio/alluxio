package alluxio.client.cli;

import alluxio.cli.LogLevel;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LogLevelTest {
  private static AlluxioConfiguration sConf =
          new InstancedConfiguration(ConfigurationUtils.defaults());

  // test URL to master rest service
  @Test
  public void parseMaster() throws Exception {
    List<LogLevel.TargetInfo> targets = LogLevel.getTargetInfos(new String[]{LogLevel.ROLE_MASTER}, sConf);
    assertEquals(targets.size(), 1);
    LogLevel.TargetInfo master = targets.get(0);
    assertEquals(master.getRole(), LogLevel.ROLE_MASTER);
  }

  // test URL to workers rest service

  // test URL to job master rest service

  // test URL to job workers rest service

  // test URL to comma separated host:ports

  // test URL to unrecognized host:port

  // test with combined comma separated param

  // test combined host:port failure case
}
