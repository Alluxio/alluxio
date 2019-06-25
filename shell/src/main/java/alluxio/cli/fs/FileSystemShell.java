/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.fs;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.conf.Source;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class for handling command line inputs.
 */
@NotThreadSafe
public final class FileSystemShell extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemShell.class);

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
    int ret;
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    if (!ConfigurationUtils.masterHostConfigured(conf)
        && argv.length > 0 && !argv[0].equals("help")) {
      System.out.println(ConfigurationUtils.getMasterHostNotConfiguredMessage("Alluxio fs shell"));
      System.exit(1);
    }

    // Reduce the RPC retry max duration to fall earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    try (FileSystemShell shell = new FileSystemShell(conf)) {
      ret = shell.run(argv);
    }
    System.exit(ret);
  }

  /**
   * Creates a new instance of {@link FileSystemShell}.
   *
   * @param alluxioConf Alluxio configuration
   */
  public FileSystemShell(InstancedConfiguration alluxioConf) {
    super(CMD_ALIAS, UNSTABLE_ALIAS, alluxioConf);
  }

  @Override
  protected String getShellName() {
    return "fs";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    return FileSystemShellUtils
        .loadCommands(
          mCloser.register(FileSystemContext.create(mConfiguration)));
  }
}
