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

package alluxio.cli.job;

import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class for handling job command line inputs.
 */
@NotThreadSafe
public final class JobShell extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(JobShell.class);

  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.<String, String[]>builder()
      .build();

  /**
   * Main method, starts a new JobShell.
   *
   * @param argv array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    int ret;
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    if (!ConfigurationUtils.masterHostConfigured(conf) && argv.length > 0) {
      System.out.println(String.format(
          "Cannot run alluxio job shell; master hostname is not "
              + "configured. Please modify %s to either set %s or configure zookeeper with "
              + "%s=true and %s=[comma-separated zookeeper master addresses]",
          Constants.SITE_PROPERTIES, PropertyKey.MASTER_HOSTNAME.toString(),
          PropertyKey.ZOOKEEPER_ENABLED.toString(), PropertyKey.ZOOKEEPER_ADDRESS.toString()));
      System.exit(1);
    }

    try (JobShell shell = new JobShell(conf)) {
      ret = shell.run(argv);
    }
    System.exit(ret);
  }

  /**
   * Creates a new instance of {@link JobShell}.
   *
   * @param alluxioConf Alluxio configuration
   */
  public JobShell(InstancedConfiguration alluxioConf) {
    super(CMD_ALIAS, alluxioConf);
  }

  @Override
  protected String getShellName() {
    return "job";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    return CommandUtils.loadCommands(JobShell.class.getPackage().getName(),
        new Class[] {FileSystemContext.class},
        new Object[] {FileSystemContext.create(mConfiguration)});
  }
}
