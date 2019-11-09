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

import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Properties;

/**
 * A command which can be used to attach a UDB to the Alluxio master's table service.
 */
public class AttachDatabaseCommand extends AbstractTableCommand {
  private static final String COMMAND_NAME = "attachdb";

  private static final Option OPTION_OPTION = Option.builder("o")
      .longOpt("option")
      .required(false)
      .hasArg(true)
      .numberOfArgs(2)
      .argName("key=value")
      .valueSeparator('=')
      .desc("options associated with this UDB")
      .build();
  private static final Option DB_OPTION = Option.builder()
      .longOpt("db")
      .required(false)
      .hasArg(true)
      .numberOfArgs(1)
      .argName("alluxio db name")
      .desc("The name of the db in Alluxio. If unset, will use the udb db name.")
      .build();

  /**
   * Creates a new instance of {@link AttachDatabaseCommand}.
   *
   * @param conf alluxio configuration
   * @param client the table master client used to make RPCs
   */
  public AttachDatabaseCommand(AlluxioConfiguration conf, TableMasterClient client) {
    super(conf, client);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(OPTION_OPTION).addOption(DB_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 3);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioStatusException {
    String[] args = cl.getArgs();
    String udbType = args[0];
    String udbConnectionUri = args[1];
    String udbDbName = args[2];
    String dbName = udbDbName;

    if (cl.hasOption(DB_OPTION.getLongOpt())) {
      String optDbName = cl.getOptionValue(DB_OPTION.getLongOpt());
      if (optDbName != null && !optDbName.isEmpty()) {
        dbName = optDbName;
      }
    }

    Properties p = cl.getOptionProperties(OPTION_OPTION.getOpt());
    mClient.attachDatabase(udbType, udbConnectionUri, udbDbName, dbName, Maps.fromProperties(p));
    return 0;
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public String getDescription() {
    return "Attaches a database to the Alluxio catalog from an under DB";
  }

  @Override
  public String getUsage() {
    return "attachdb [-o|--option <key=value>] [--db <alluxio db name>] "
        + "<udb type> <udb connection uri> <udb db name>";
  }
}
