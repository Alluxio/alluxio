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
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.table.Constraint;
import alluxio.grpc.table.Partition;
import alluxio.grpc.table.TableInfo;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * List information about attached databases and tables in the Alluxio catalog.
 *
 * This command has similar function to what a {@code SHOW TABLES} or {@code SHOW DATABASES} query
 * would return but makes it easier to query on a local machine what the Alluxio catalog
 * currently stores without needing to boot up a presto instance.
 */
public class ListDatabasesCommand extends AbstractTableCommand {

  /**
   * Creates a new instance of {@link ListDatabasesCommand}.
   *
   * @param conf alluxio configuration
   * @param client the table master client
   * @param fsContext the filesystem of Alluxio
   */
  public ListDatabasesCommand(AlluxioConfiguration conf, TableMasterClient client,
      FileSystemContext fsContext) {
    super(conf, client, fsContext);
  }

  @Override
  public String getCommandName() {
    return "ls";
  }

  @Override
  public String getDescription() {
    return "list information about attached databases";
  }

  @Override
  public String getUsage() {
    return "ls [<db name> [<table name>]]";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 2);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    // Guaranteed to have 0 to 2 args.
    String[] args = cl.getArgs();
    switch (args.length) {
      case 0:
        return listDatabases();
      case 1:
        return listTables(args[0]);
      case 2:
        return listTable(args[0], args[1]);
      default:
        return 1;
    }
  }

  /**
   * Print attached databases to stdout.
   *
   * @return 0 on success, any non-zero value otherwise
   */
  public int listDatabases() throws AlluxioStatusException, IOException {
    sortAndPrint(new ArrayList<>(mClient.getAllDatabases()));
    return 0;
  }

  /**
   * Print list of tables stdout.
   *
   * @param db the database to list the tables of
   * @return 0 on success, any non-zero value otherwise
   */
  public int listTables(String db) throws AlluxioStatusException {
    sortAndPrint(new ArrayList<>(mClient.getAllTables(db)));
    return 0;
  }

  private void sortAndPrint(List<String> items) {
    items.sort(String::compareTo);
    items.forEach(System.out::println);
  }

  /**
   * Print table information to stdout.
   *
   * @param db the database the table exists in
   * @param tableName the name of the table to dump information for
   * @return 0 on success, any non-zero value otherwise
   */
  public int listTable(String db, String tableName) throws AlluxioStatusException {
    TableInfo table = mClient.getTable(db, tableName);
    System.out.println(table);
    List<Partition> partitionList = mClient.readTable(db, tableName,
        Constraint.getDefaultInstance());
    partitionList.forEach(System.out::println);
    return 0;
  }
}
