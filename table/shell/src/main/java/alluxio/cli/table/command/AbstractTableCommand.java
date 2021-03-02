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

import alluxio.cli.fs.command.AbstractDistributedJobCommand;
import alluxio.cli.table.TableShell;
import alluxio.client.file.FileSystemContext;
import alluxio.client.table.TableMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * A class which should be extended when implementing commands for the
 * {@link TableShell}.
 */
public abstract class AbstractTableCommand extends AbstractDistributedJobCommand {

  protected final AlluxioConfiguration mConf;
  protected final TableMasterClient mClient;

  /**
   * Creates a new instance of {@link AbstractTableCommand}.
   *
   * @param conf the alluxio configuration
   * @param client the client interface which can be used to make RPCs against the table master
   * @param fsContext the filesystem of Alluxio
   */
  public AbstractTableCommand(AlluxioConfiguration conf, TableMasterClient client,
      FileSystemContext fsContext) {
    super(fsContext);
    mConf = conf;
    mClient = client;
  }

  @Override
  public abstract String getCommandName();

  @Override
  public abstract void validateArgs(CommandLine cl) throws InvalidArgumentException;

  @Override
  public abstract int run(CommandLine cl) throws AlluxioException, IOException;
}
