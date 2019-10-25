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

package alluxio.cli.table.command;

import alluxio.cli.CommandUtils;
import alluxio.client.table.TableMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

/**
 * A command which can be used to sync a database with the underlying udb.
 */
public class SyncDatabaseCommand extends AbstractTableCommand{

  /**
   * Creates a new instance of {@link SyncDatabaseCommand}.
   *
   * @param conf alluxio configuration
   * @param client the table master client
   */
  public SyncDatabaseCommand(AlluxioConfiguration conf, TableMasterClient client) {
    super(conf, client);
  }

  @Override
  public void validateArgs(CommandLine cli) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cli, 1);
  }

  @Override
  public String getCommandName() {
    return "sync";
  }

  @Override
  public String getUsage() {
    return "sync <db name>";
  }

  @Override
  public String getDescription() {
    return "Sync a database with the given name with the UDB";
  }

  @Override
  public int run(CommandLine cli) throws AlluxioStatusException {
    mClient.syncDatabase(cli.getArgs()[0]);
    return 0;
  }
}
