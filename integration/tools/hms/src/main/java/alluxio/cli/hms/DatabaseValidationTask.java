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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

/**
 * A task which, given a metastore client can retrieve information from a user-specified
 * database.
 */
public class DatabaseValidationTask extends MetastoreValidationTask<IMetaStoreClient, String> {

  private final String mDbName;

  /**
   * Create a new instance of {@link DatabaseValidationTask}.
   *
   * @param dbName the database name to get information from
   * @param prereq a pre-requisite task that provides a metastore client
   */
  public DatabaseValidationTask(String dbName,
      MetastoreValidationTask<?, IMetaStoreClient> prereq) {
    super(prereq);
    mDbName = dbName;
  }

  @Override
  Pair<ValidationTaskResult, String> getValidationWithResult() throws InterruptedException {
    if (mInputTask == null) {
      return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
          "Did not have a pre-requisite task to obtain HMS client for database check", ""), null);
    }
    Pair<ValidationTaskResult, IMetaStoreClient> res = mInputTask.getValidationWithResult();
    if (res.getFirst().getState() != ValidationUtils.State.OK) {
      return new Pair<>(res.getFirst(), null);
    }
    IMetaStoreClient client = res.getSecond();
    try {
      Database database = client.getDatabase(mDbName);
      return new Pair<>(new ValidationTaskResult(ValidationUtils.State.OK, getName(),
          String.format("Database (name: %s, description: %s)",
              database.getName(), database.getDescription()), ""), mDbName);
    } catch (NoSuchObjectException e) {
      return new Pair<>(
          new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
              ValidationUtils.getErrorInfo(e),
              "Please make sure the given database name is valid "
                  + "and existing in the target hive metastore"), null);
    } catch (Throwable t) {
      return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
          ValidationUtils.getErrorInfo(t), "Failed to get database from remote hive metastore"),
          null);
    } finally {
      client.close();
    }
  }

  @Override
  public String getName() {
    return "getDatabaseCheck";
  }
}
