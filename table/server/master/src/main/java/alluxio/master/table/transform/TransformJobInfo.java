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

package alluxio.master.table.transform;

import alluxio.collections.Pair;
import alluxio.job.wire.Status;
import alluxio.table.common.Layout;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotNull;

/**
 * Information kept for a transformation job.
 */
@ThreadSafe
public final class TransformJobInfo {
  private final Pair<String, String> mDbTable;
  private final String mDefinition;
  private final Map<String, Layout> mTransformedLayouts;
  private final long mJobId;
  private volatile Status mJobStatus;
  private volatile String mJobErrorMessage;

  /**
   * The default job status on construction is {@link Status#RUNNING}.
   *
   * @param db the database name
   * @param table the table name
   * @param definition the transformation definition
   * @param jobId the job ID
   * @param transformedLayouts the mapping from a partition spec to its transformed layout
   */
  public TransformJobInfo(@NotNull String db, @NotNull String table, @NotNull String definition,
      long jobId, @NotNull Map<String, Layout> transformedLayouts) {
    Preconditions.checkNotNull(db, "db");
    Preconditions.checkNotNull(table, "table");
    Preconditions.checkNotNull(definition, "definition");
    Preconditions.checkNotNull(transformedLayouts, "transformedLayouts");

    mDbTable = new Pair<>(db, table);
    mDefinition = definition;
    mTransformedLayouts = Collections.unmodifiableMap(transformedLayouts);
    mJobId = jobId;
    mJobStatus = Status.RUNNING;
    mJobErrorMessage = "";
  }

  /**
   * @return the database name
   */
  public String getDb() {
    return mDbTable.getFirst();
  }

  /**
   * @return the table name
   */
  public String getTable() {
    return mDbTable.getSecond();
  }

  /**
   * @return the (db, table) pair
   */
  public Pair<String, String> getDbTable() {
    return mDbTable;
  }

  /**
   * @return the transformation definition
   */
  public String getDefinition() {
    return mDefinition;
  }

  /**
   * @return a read-only mapping from a partition spec to its transformed layout
   */
  public Map<String, Layout> getTransformedLayouts() {
    return mTransformedLayouts;
  }

  /**
   * @return the job ID
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @return the job status
   */
  public Status getJobStatus() {
    return mJobStatus;
  }

  /**
   * Sets the job status.
   *
   * @param status the job status
   */
  public void setJobStatus(@NotNull Status status) {
    Preconditions.checkNotNull(status, "status");
    mJobStatus = status;
  }

  /**
   * @return the job error message or empty if there is no error
   */
  public String getJobErrorMessage() {
    return mJobErrorMessage;
  }

  /**
   * Sets the job error message.
   *
   * @param error the error
   */
  public void setJobErrorMessage(@NotNull String error) {
    Preconditions.checkNotNull(error, "error");
    mJobErrorMessage = error;
  }

  /**
   * @return the proto representation
   */
  public alluxio.grpc.table.TransformJobInfo toProto() {
    return alluxio.grpc.table.TransformJobInfo.newBuilder()
        .setDbName(getDb())
        .setTableName(getTable())
        .setDefinition(getDefinition())
        .setJobId(getJobId())
        .setJobStatus(getJobStatus().toProto())
        .setJobError(getJobErrorMessage())
        .build();
  }
}
