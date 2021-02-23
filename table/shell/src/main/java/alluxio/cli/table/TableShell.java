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

package alluxio.cli.table;

import alluxio.ClientContext;
import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.table.TableMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.master.MasterClientContext;
import alluxio.util.ConfigurationUtils;

import java.util.Map;

/**
 * A shell implementation which is used to load the commands for interacting with the Alluxio
 * table service.
 */
public class TableShell extends AbstractShell {
  private static final String SHELL_NAME = "table";

  /**
   * Construct a new instance of {@link TableShell}.
   *
   * @param conf the Alluxio configuration to use when instantiating the shell
   */
  public TableShell(InstancedConfiguration conf) {
    super(null, null, conf);
  }

  /**
   * Manage Alluxio extensions.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) {
    TableShell tableShell =
        new TableShell(new InstancedConfiguration(ConfigurationUtils.defaults()));
    System.exit(tableShell.run(args));
  }

  @Override
  public Map<String, Command> loadCommands() {
    return CommandUtils.loadCommands(TableShell.class.getPackage().getName(),
        new Class[] {AlluxioConfiguration.class, TableMasterClient.class, FileSystemContext.class},
        new Object[] {mConfiguration, mCloser.register(TableMasterClient.Factory.create(
            MasterClientContext.newBuilder(ClientContext.create(mConfiguration)).build())),
            FileSystemContext.create(mConfiguration)}
        );
  }

  @Override
  public String getShellName() {
    return SHELL_NAME;
  }
}
