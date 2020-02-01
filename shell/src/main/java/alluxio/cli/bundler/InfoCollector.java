package alluxio.cli.bundler;

import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.bundler.command.AbstractInfoCollectorCommand;
import alluxio.cli.fs.FileSystemShell;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InfoCollector extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(InfoCollector.class);

  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.<String, String[]>builder()
          .build();

  // In order for a warning to be displayed for an unstable alias, it must also exist within the
  // CMD_ALIAS map.
  private static final Set<String> UNSTABLE_ALIAS = ImmutableSet.<String>builder()
          .build();

  private static Map<String, Command> sCommands;

  /**
   * Main method, starts a new FileSystemShell.
   *
   * @param argv array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    int ret = 0;

    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    FileSystemContext fsContext = FileSystemContext.create(conf);

    // Execute the Collectors one by one
    // Reduce the RPC retry max duration to fall earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    InfoCollector shell = new InfoCollector(conf);

    ret = shell.run(argv);

    System.exit(ret);
  }

  /**
   * Creates a new instance of {@link FileSystemShell}.
   *
   * @param alluxioConf Alluxio configuration
   */
  public InfoCollector(InstancedConfiguration alluxioConf) {
    super(CMD_ALIAS, UNSTABLE_ALIAS, alluxioConf);
  }

  @Override
  protected String getShellName() {
    return "infoBundle";
  }

  @Override
  // TODO(jiacheng): don't share this method
  protected Map<String, Command> loadCommands() {
    // Give each command the configuration
    Map<String, Command> commands = CommandUtils.loadCommands(InfoCollector.class.getPackage().getName(),
              new Class[] {FileSystemContext.class}, new Object[] {FileSystemContext.create(mConfiguration)});
    System.out.println(String.format("Loaded commands %s", commands));

    // Update the reference
    if (sCommands == null) {
      sCommands = commands;
    }
    return commands;
  }

  public static Map<String, Command> getIndividualCommands() {
    if (sCommands.containsKey("collectAll")) {
      sCommands.remove("collectAll");
    }

    return sCommands;
  }
}
