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

import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.client.file.FileSystem;
import alluxio.conf.Source;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class for handling command line inputs.
 */
@NotThreadSafe
public final class FileSystemShell extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemShell.class);

  protected final FileSystemContext mFilesystemContext;

  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.<String, String[]>builder()
      .put("lsr", new String[] {"ls", "-R"})
      .put("rmr", new String[] {"rm", "-R"})
      .build();

  /**
   * Main method, starts a new FileSystemShell.
   *
   * @param argv array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    int ret;
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    if (!ConfigurationUtils.masterHostConfigured(conf) && argv.length > 0 && !argv[0].equals("help")) {
      System.out.println(String.format(
          "Cannot run alluxio fs shell; master hostname is not "
              + "configured. Please modify %s to either set %s or configure zookeeper with "
              + "%s=true and %s=[comma-separated zookeeper master addresses]",
          Constants.SITE_PROPERTIES, PropertyKey.MASTER_HOSTNAME.toString(),
          PropertyKey.ZOOKEEPER_ENABLED.toString(), PropertyKey.ZOOKEEPER_ADDRESS.toString()));
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
   */
  public FileSystemShell(InstancedConfiguration conf) {
    super(CMD_ALIAS, conf);
    mFilesystemContext = FileSystemContext.create(null, mConfiguration.getProperties());
  }

  @Override
  protected String getShellName() {
    return "fs";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    return FileSystemShellUtils.loadCommands(FileSystemContext.create(null, mConfiguration));
  }
}
