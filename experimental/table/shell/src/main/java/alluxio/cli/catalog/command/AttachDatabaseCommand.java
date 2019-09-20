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

package alluxio.cli.catalog.command;

import alluxio.cli.CommandUtils;
import alluxio.client.catalog.CatalogMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;

import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Properties;

/**
 * A command which can be used to attach a UDB to the Alluxio master's catalog service.
 */
public class AttachDatabaseCommand extends AbstractCatalogCommand {
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

  /**
   * Creates a new instance of {@link AttachDatabaseCommand}.
   *
   * @param conf alluxio configuration
   * @param client the catalog master client used to make RPCs
   */
  public AttachDatabaseCommand(AlluxioConfiguration conf, CatalogMasterClient client) {
    super(conf, client);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(OPTION_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public int run(CommandLine cl) {
    String[] args = cl.getArgs();
    String dbName = args[0];
    String dbType = args[1];
    Properties p = cl.getOptionProperties(OPTION_OPTION.getOpt());
    try {
      mClient.attachDatabase(dbName, dbType, Maps.fromProperties(p));
    } catch (AlluxioStatusException e) {
      e.printStackTrace();
      return 1;
    }
    return 0;
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public String getDescription() {
    return "Attaches a database to the alluxio catalog service from an under store DB";
  }

  @Override
  public String getUsage() {
    return "attachdb <dbName> <dbType> [-o|--option <option>]";
  }
}
