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
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Transforms a structured table in Alluxio.
 */
@ThreadSafe
public final class TransformTableCommand extends AbstractTableCommand {
  private static final Logger LOG = LoggerFactory.getLogger(TransformTableCommand.class);

  private static final String COMMAND_NAME = "transform";

  /**
   * creates the command.
   *
   * @param conf alluxio configuration
   * @param client the table master client used to make RPCs
   */
  public TransformTableCommand(AlluxioConfiguration conf, TableMasterClient client) {
    super(conf, client);
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public String getUsage() {
    return COMMAND_NAME + " <database_name> <table_name> <definition>";
  }

  @Override
  public String getDescription() {
    return "Transform files representing a structured table under an Alluxio directory."
        + "\n\n"
        + "After transformation, the table's metadata will be updated "
        + "so that queries will query against the transformed table."
        + "\n\n"
        + "Files are compacted into a specified number of new files, "
        + "if the number of existing files is less than the specified number, "
        + "no compaction happens."
        + "\n\n"
        + "definition is a DSL like 'write(hive).option(hive.num.files, 100).'";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 3);
  }

  @Override
  public int run(CommandLine cl) throws IOException, AlluxioException {
    String[] args = cl.getArgs();
    String databaseName = args[0];
    String tableName = args[1];
    String definition = args[2];

    long jobId = mClient.transformTable(databaseName, tableName, definition);
    System.out.println("Started transformation job with job ID " + jobId);
    return 0;
  }
}
