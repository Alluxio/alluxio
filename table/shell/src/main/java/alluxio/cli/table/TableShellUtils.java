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

import alluxio.grpc.table.SyncStatus;

import org.slf4j.Logger;

import java.util.Map;

/**
 * A utility class for the table shell.
 */
public class TableShellUtils {
  private static final String SHELL_NAME = "table";

  private TableShellUtils() {} // prevent instantiation

  /**
   * Prints out the sync status.
   *
   * @param status the sync status to print
   * @param logger the logger to errors log to
   * @param maxErrors a max number of errors to print to stdout
   */
  public static void printSyncStatus(SyncStatus status, Logger logger, int maxErrors) {
    System.out.println("# Tables ignored: " + status.getTablesIgnoredCount());
    System.out.println("# Tables unchanged: " + status.getTablesUnchangedCount());
    System.out.println("# Tables updated: " + status.getTablesUpdatedCount());
    System.out.println("# Tables removed: " + status.getTablesRemovedCount());
    System.out.println("# Tables with errors: " + status.getTablesErrorsCount());

    // write the tables to the log
    for (String table : status.getTablesIgnoredList()) {
      logger.info("Table ignored: {}", table);
    }
    for (String table : status.getTablesUnchangedList()) {
      logger.info("Table unchanged: {}", table);
    }
    for (String table : status.getTablesUpdatedList()) {
      logger.info("Table updated: {}", table);
    }
    for (String table : status.getTablesRemovedList()) {
      logger.info("Table removed: {}", table);
    }

    if (status.getTablesErrorsCount() > 0) {
      System.out.println("\nSync errors: ");
    }
    int count = 0;
    for (Map.Entry<String, String> entry : status.getTablesErrorsMap().entrySet()) {
      String message =
          String.format("Table %s failed to sync: %s", entry.getKey(), entry.getValue());
      if (count < maxErrors) {
        System.out.println(message);
      } else if (count == maxErrors) {
        System.out.println("... (remaining can be found in the log)");
      }
      logger.error(message);
      count++;
    }
  }
}
