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

package alluxio.cli;

import alluxio.Constants;
import alluxio.cli.ValidationUtils.State;
import alluxio.cli.hms.CreateHmsClientValidationTask;
import alluxio.cli.hms.DatabaseValidationTask;
import alluxio.cli.hms.UriCheckTask;
import alluxio.cli.hms.TableValidationTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Run tests against an existing hive metastore.
 */
public class HmsValidationTool implements ValidationTool {
  private static final Logger LOG = LoggerFactory.getLogger(HmsValidationTool.class);

  // Default hive metastore client socket timeout in minutes
  public static final int DEFAULT_SOCKET_TIMEOUT_MINUTES = 12;
  public static final String DEFAULT_DATABASE = "default";

  private final String mMetastoreUri;
  private final String mDatabase;
  private final String mTables;
  private final int mSocketTimeoutMinutes;
  private final Map<String, ValidationTask> mTasks;

  /**
   * Constructs a new {@link HmsValidationTool}.
   *
   * @param metastoreUri hive metastore uris
   * @param database database to run tests against
   * @param tables tables to run tests against
   * @param socketTimeout socket time of hms operations
   */
  private HmsValidationTool(String metastoreUri, String database, String tables,
      int socketTimeout) {
    mMetastoreUri = metastoreUri;
    mDatabase = database == null || database.isEmpty() ? DEFAULT_DATABASE : database;
    mTables = tables;
    mSocketTimeoutMinutes = socketTimeout > 0 ? socketTimeout : DEFAULT_SOCKET_TIMEOUT_MINUTES;
    mTasks = new HashMap<>();
    UriCheckTask uriCheck = new UriCheckTask(mMetastoreUri,
        mSocketTimeoutMinutes * Constants.MINUTE_MS);
    CreateHmsClientValidationTask clientTask =
        new CreateHmsClientValidationTask(
            mSocketTimeoutMinutes * Constants.MINUTE_SECONDS, uriCheck);
    DatabaseValidationTask dbTask = new DatabaseValidationTask(mDatabase, clientTask);
    TableValidationTask tableTask = new TableValidationTask(mDatabase, mTables, clientTask);
    mTasks.put(uriCheck.getName(), uriCheck);
    mTasks.put(clientTask.getName(), clientTask);
    mTasks.put(dbTask.getName(), dbTask);
    mTasks.put(tableTask.getName(), tableTask);
  }

  /**
   * Creates an instance of {@link HmsValidationTool}.
   *
   * @param configMap the hms validation tool config map
   * @return the new instance
   */
  public static HmsValidationTool create(Map<Object, Object> configMap) {
    String metastoreUri = "";
    String database = DEFAULT_DATABASE;
    String tables = "";
    int socketTimeout = DEFAULT_SOCKET_TIMEOUT_MINUTES;
    try {
      metastoreUri = (String) configMap
          .getOrDefault(ValidationConfig.METASTORE_URI_CONFIG_NAME, "");
      database = (String) configMap
          .getOrDefault(ValidationConfig.DATABASE_CONFIG_NAME, DEFAULT_DATABASE);
      tables = (String) configMap
          .getOrDefault(ValidationConfig.TABLES_CONFIG_NAME, "");
      socketTimeout = Integer.parseInt(configMap
          .getOrDefault(ValidationConfig.SOCKET_TIMEOUT_CONFIG_NAME, DEFAULT_SOCKET_TIMEOUT_MINUTES)
          .toString());
    } catch (RuntimeException e) {
      // Try not to throw exception on the construction function
      // The hms validation tool itself should return failed message if the given config is invalid
      LOG.error("Failed to process hms validation tool config from config map {}",
          configMap, e);
    }
    return new HmsValidationTool(metastoreUri, database, tables, socketTimeout);
  }

  @Override
  public Map<String, ValidationTask> getTasks() {
    return mTasks;
  }

  @Override
  public List<ValidationTaskResult> runAllTests() {
    return mTasks.values().stream().map(t -> {
      try {
        return t.validate(new HashMap<>());
      } catch (InterruptedException e) {
        return new ValidationTaskResult(State.FAILED, t.getName(),
            "Task interrupted while running", "");
      }
    }).collect(Collectors.toList());
  }
}
