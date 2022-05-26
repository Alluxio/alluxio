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

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.command.DistributedLoadUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.table.TableMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.table.common.CatalogPathUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads table into Alluxio space, makes it resident in memory.
 */
@ThreadSafe
@PublicApi
public class LoadTableCommand extends AbstractTableCommand {

  /**
   *  Constructs a new instance to load table into Alluxio space.
   *
   * @param conf      the alluxio configuration
   * @param client    the client interface which can be used to make RPCs against the table master
   * @param fsContext the filesystem of Alluxio
   */
  public LoadTableCommand(AlluxioConfiguration conf, TableMasterClient client,
      FileSystemContext fsContext) {
    super(conf, client, fsContext);
  }

  @Override
  public String getUsage() {
    return "load  "
        + DistributedLoadUtils.getDistLoadCommonUsage()
        + "<db name> <table name>";
  }

  @Override
  public String getCommandName() {
    return "load";
  }

  @Override
  public String getDescription() {
    return "Loads table into Alluxio space. Currently only support hive table.";
  }

  @Override
  public Options getOptions() {
    return DistributedLoadUtils.getDistLoadCommonOptions();
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    System.out.println("***Tips：Load table command only support hive table for now.***");
    String[] args = cl.getArgs();
    String dbName = args[0];
    String tableName = args[1];
    if (!tableExists(dbName, tableName)) {
      System.out.printf("Failed to load table %s.%s: table is not exit.%n", dbName, tableName);
      return 0;
    }
    // Only support hive table for now.
    String udbType = "hive";
    // To load table into Alluxio space, we get the SDS table's Alluxio parent path first and
    // then load data under the path. For now, each table have one single parent path generated
    // by CatalogPathUtils#getTablePathUdb.
    // The parent path is mounted by SDS, it's mapping of the under table's UFS path in Alluxio.
    // e.g.
    //                                                   attached
    // [SDS]default.test                                 <===== [hive]default.test
    //                                                    mount
    // [SDS]alluxio:///catalog/default/tables/test/hive/ <===== [hive]hdfs:///.../default.db/test/
    // PLEASE NOTE: If Alluxio support different parent path, this statement can not guaranteed
    // to be correct.
    AlluxioURI path = CatalogPathUtils.getTablePathUdb(dbName, tableName, udbType);
    System.out.printf("Loading table %s.%s...%n", dbName, tableName);
    return DistributedLoadUtils.distributedLoad(this, cl, path.getPath());
  }

  private boolean tableExists(String dbName, String tableName) {
    try {
      // If getTable method called succeed, the table is exists.
      mClient.getTable(dbName, tableName);
      return true;
    } catch (AlluxioStatusException e) {
      return false;
    }
  }
}
