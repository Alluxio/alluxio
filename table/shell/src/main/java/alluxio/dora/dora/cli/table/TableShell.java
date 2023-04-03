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

package alluxio.dora.dora.cli.table;

import alluxio.dora.dora.ClientContext;
import alluxio.dora.dora.cli.AbstractShell;
import alluxio.dora.dora.cli.Command;
import alluxio.dora.dora.cli.CommandUtils;
import alluxio.dora.dora.client.file.FileSystemContext;
import alluxio.dora.dora.client.table.TableMasterClient;
import alluxio.dora.dora.conf.AlluxioConfiguration;
import alluxio.dora.dora.conf.Configuration;
import alluxio.dora.dora.master.MasterClientContext;

import java.util.Map;

/**
 * A shell implementation which is used to load the commands for interacting with the Alluxio
 * table service.
 */
public class TableShell extends AbstractShell {
  private static final String SHELL_NAME = "table";

  /**
   * Construct a new instance of {@link TableShell}.
   */
  public TableShell() {
    super(null, null, Configuration.global());
  }

  /**
   * Manage Alluxio extensions.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) {
    TableShell tableShell = new TableShell();
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
