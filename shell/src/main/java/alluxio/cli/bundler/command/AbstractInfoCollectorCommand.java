package alluxio.cli.bundler.command;

import alluxio.cli.Command;
import alluxio.cli.bundler.InfoCollector;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;

import javax.annotation.Nullable;

public abstract class AbstractInfoCollectorCommand implements Command {
  InstancedConfiguration mConf;
  public AbstractInfoCollectorCommand(@Nullable InstancedConfiguration conf) {
    if (conf == null) {
      mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
    } else {
      mConf = conf;
    }
  }

  String getWorkingDirectory() {
    return InfoCollector.getWorkingDir();
  }

  // TODO(jiacheng): check for previously complete work
}
