package alluxio.cli.bundler;

import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShell;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InfoCollectorAll extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(InfoCollectorAll.class);

  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.<String, String[]>builder()
          .build();

  // In order for a warning to be displayed for an unstable alias, it must also exist within the
  // CMD_ALIAS map.
  private static final Set<String> UNSTABLE_ALIAS = ImmutableSet.<String>builder()
          .build();

  /**
   * Creates a new instance of {@link FileSystemShell}.
   *
   * @param alluxioConf Alluxio configuration
   */
  public InfoCollectorAll(InstancedConfiguration alluxioConf) {
    super(CMD_ALIAS, UNSTABLE_ALIAS, alluxioConf);
  }

  public Set<String> getHosts() {
    // File
    String confDirPath = mConfiguration.get(PropertyKey.CONF_DIR);
    Set<String> hosts = new HashSet<>();
    hosts.addAll(CommandUtils.readNodeList(confDirPath, "masters"));
    hosts.addAll(CommandUtils.readNodeList(confDirPath, "workers"));

    return hosts;
  }

  /**
   * Main method, starts a new FileSystemShell.
   *
   * @param argv array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    int ret = 0;


    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // Execute the Collectors one by one
    // Reduce the RPC retry max duration to fall earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    InfoCollectorAll shellAll = new InfoCollectorAll(conf);

    // For each host execute
    // TODO(jiacheng): get hosts from static util call
    Set<String> allHosts = shellAll.getHosts();

    // Threadpool to execute on each host


    for (String host : allHosts) {
      LOG.info(String.format("Execute InfoCollector on host %s", host));

      // TODO(jiacheng): Execute InfoCollector
      // TODO(jiacheng): 1. Test local invocation no ssh
      // TODO(jiacheng): 2. Test local invocation ssh
      // TODO(jiacheng): 3. Test multi nodes invocation ssh
      // TODO(jiacheng): 4. Test threadpool


      InfoCollector shell = new InfoCollector(conf);
      shell.run(argv);
    }

    System.exit(ret);
  }

  @Override
  protected String getShellName() {
    return "collectInfoAll";
  }

  @Override
  //TODO(jiacheng): load commands
  protected Map<String, Command> loadCommands() {
    return null;
  }
}
