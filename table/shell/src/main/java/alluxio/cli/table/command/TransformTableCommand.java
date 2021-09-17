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
import alluxio.client.file.FileSystemContext;
import alluxio.client.table.TableMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
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

  private static final Option DEFINITION_OPTION = Option.builder("d")
      .longOpt("definition")
      .required(false)
      .hasArg(true)
      .numberOfArgs(1)
      .build();

  /**
   * creates the command.
   *
   * @param conf alluxio configuration
   * @param client the table master client used to make RPCs
   * @param fsContext the filesystem of Alluxio
   */
  public TransformTableCommand(AlluxioConfiguration conf, TableMasterClient client,
      FileSystemContext fsContext) {
    super(conf, client, fsContext);
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public String getUsage() {
    return COMMAND_NAME + " <db name> <table name> [-d <definition>]";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(DEFINITION_OPTION);
  }

  @Override
  public String getDescription() {
    return "Transform files representing a structured table under an Alluxio directory."
        + "\n\n"
        + "Files are coalesced and converted to parquet format."
        + "\n";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public int run(CommandLine cl) throws IOException, AlluxioException {
    String[] args = cl.getArgs();
    String databaseName = args[0];
    String tableName = args[1];
    String definition = "";

    if (cl.hasOption(DEFINITION_OPTION.getLongOpt())) {
      String optDefinition = cl.getOptionValue(DEFINITION_OPTION.getLongOpt());
      if (optDefinition != null && !optDefinition.isEmpty()) {
        definition = optDefinition.trim();
      }
    }

    long jobId = mClient.transformTable(databaseName, tableName, definition);
    System.out.println("Started transformation job with job ID " + jobId + ", "
        + "you can monitor the status of the job with "
        + "'./bin/alluxio table transformStatus " + jobId + "'.");
    return 0;
  }
}
