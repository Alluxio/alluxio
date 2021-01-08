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

package alluxio.cli.hms;

import alluxio.cli.ValidationTaskResult;
import alluxio.cli.ValidationUtils;
import alluxio.collections.Pair;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A task which validates table information and schemas can be obtained from the Hive Metastore.
 */
public class TableValidationTask extends MetastoreValidationTask<IMetaStoreClient, String> {

  // The maximum number of table objects that this test will get.
  // Used to avoid issuing too many calls to the hive metastore
  // which may need a long time based on network conditions
  private static final int GET_TABLE_OBJECT_THRESHOLD = 5;

  private final String mTables;
  private final String mDatabase;

  /**
   * Create a new metastore validation task.
   *
   * @param database the database containing the desired tables
   * @param tableNames a single table name, or comma-delimited list of table names to check for;
   *                   If null or empty, retrieves all tables in the database
   * @param prereq the pre-requisite task supplying the metastore client
   */
  public TableValidationTask(String database, String tableNames,
      MetastoreValidationTask<?, IMetaStoreClient> prereq) {
    super(prereq);
    mTables = tableNames;
    mDatabase = database;
  }

  @Override
  Pair<ValidationTaskResult, String> getValidationWithResult() throws InterruptedException {
    if (mInputTask == null) {
      return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
          "Did not have a pre-requisite task to obtain HMS client for table check", ""), null);
    }
    Pair<ValidationTaskResult, IMetaStoreClient> res = mInputTask.getValidationWithResult();
    if (res.getFirst().getState() != ValidationUtils.State.OK) {
      return new Pair<>(res.getFirst(), null);
    }
    IMetaStoreClient client = res.getSecond();
    try {
      if (mTables != null && !mTables.isEmpty()) {
        return new Pair<>(getTableSchemaTest(client, Arrays.asList(mTables.split(","))), null);
      } else {
        return new Pair<>(getAllTableInfoTest(client), null);
      }
    } finally {
      client.close();
    }
  }

  private ValidationTaskResult getAllTableInfoTest(IMetaStoreClient client) {
    List<String> tables;
    String testName = "GetAllTables";
    try {
      tables = client.getAllTables(mDatabase);
    } catch (Throwable t) {
      return addThrowableWarning(testName, t, "Database: " + mDatabase);
    }
    if (tables.size() == 0) {
      return new ValidationTaskResult(ValidationUtils.State.OK, testName,
          String.format("Database (name: %s, tables: %s)", mDatabase, tables.toString()), "");
    }
    if (tables.size() > GET_TABLE_OBJECT_THRESHOLD) {
      tables = tables.subList(0, GET_TABLE_OBJECT_THRESHOLD);
    }
    return getTableSchemaTest(client, tables);
  }

  private ValidationTaskResult getTableSchemaTest(IMetaStoreClient client,
      List<String> tableNames) {
    try {
      StringBuilder tableFieldsOutput = new StringBuilder();
      for (String table : tableNames) {
        String tableFields = client.getSchema(mDatabase, table).stream()
            .map(FieldSchema::getName).collect(Collectors.joining(","));
        tableFieldsOutput.append(String
            .format("Table (name: %s, fields: %s)%n", table, tableFields));
      }
      return new ValidationTaskResult(ValidationUtils.State.OK, getName(),
          tableFieldsOutput.toString(), "");
    } catch (Throwable t) {
      return addThrowableWarning(getName(), t, "tables: " + String.join(",", tableNames));
    }
  }

  private ValidationTaskResult addThrowableWarning(String opName, Throwable t, String opTarget) {
    ValidationTaskResult taskResult = new ValidationTaskResult()
        .setState(ValidationUtils.State.WARNING).setName(opName)
        .setOutput(ValidationUtils.getErrorInfo(t));
    if (t instanceof InvalidOperationException) {
      taskResult.setAdvice(opName + " is invalid");
    } else if (t instanceof UnknownDBException) {
      taskResult.setAdvice("Please make sure the given database name is valid "
          + "and existing in the target hive metastore");
    } else if (t instanceof UnknownTableException) {
      taskResult.setAdvice("Please make sure the given table names are valid "
          + "and existing in the target hive metastore");
    } else {
      taskResult.setAdvice(String.format("Failed to run %s (%s)", opName, opTarget));
    }
    return taskResult;
  }

  @Override
  public String getName() {
    return "TableValidationCheckTask";
  }
}
