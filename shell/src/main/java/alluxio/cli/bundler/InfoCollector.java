package alluxio.cli.bundler;

import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.fs.FileSystemShell;
import alluxio.cli.fs.FileSystemShellUtils;
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
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public class InfoCollector extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(InfoCollector.class);

  // TODO(jiacheng): alias?
  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.<String, String[]>builder()
          .put("umount", new String[]{"unmount"})
          .build();

  // In order for a warning to be displayed for an unstable alias, it must also exist within the
  // CMD_ALIAS map.
  private static final Set<String> UNSTABLE_ALIAS = ImmutableSet.<String>builder()
          .build();

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
    InfoCollector shell = new InfoCollector(conf);
    // TODO(jiacheng): For each InfoCollector run it
    String[] commandStrs = new String[]{"metric", "config", "alluxio", "info", "env"};
    for (String s : commandStrs) {
      argv[0] = s;
      // TODO(jiacheng): What is the ret value?
      ret = shell.run(argv);
    }

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
    return "fs";
  }

  @Override
  // TODO(jiacheng): Load commands
  protected Map<String, Command> loadCommands() {
    // Give each command the configuration
    return FileSystemShellUtils
            .loadCommands(
                    mCloser.register(FileSystemContext.create(mConfiguration)));
  }

  public static String getWorkingDir() {
    // TODO(jiacheng): what is the dir
    // hostname
    // params

    // create dir

    return null;
  }
}
