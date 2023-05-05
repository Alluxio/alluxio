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

package alluxio.metrics;

/**
 * Metric information.
 */
public final class MetricInfo {
  /**
   * The list of under filesystem operations triggered by master or workers.
   */
  public enum UfsOps {
    CLOSE("Close"),
    CONNECT_FROM_MASTER("ConnectFromMaster"),
    CONNECT_FROM_WORKER("ConnectFromWorker"),
    CREATE("Create"),
    DELETE_DIRECTORY("DeleteDirectory"),
    DELETE_FILE("DeleteFile"),
    EXISTS("Exists"),
    GET_BLOCK_SIZE_BYTE("GetBlockSizeByte"),
    GET_DIRECTORY_STATUS_("GetDirectoryStatus"),
    GET_FILE_LOCATIONS("GetFileLocations"),
    GET_FILE_STATUS("GetFileStatus"),
    GET_FINGERPRINT("GetFingerprint"),
    GET_SPACE("GetSpace"),
    GET_STATUS("GetStatus"),
    IS_DIRECTORY("IsDirectory"),
    IS_FILE("IsFile"),
    LIST_STATUS("ListStatus"),
    MKDIRS("Mkdirs"),
    OPEN("Open"),
    RENAME_DIRECTORY("RenameDirectory"),
    RENAME_FILE("RenameFile"),
    SET_OWNER("SetOwner"),
    SET_MODE("SetMode");

    private String mValue;

    /**
     * Creates the UFS operation type.
     *
     * @param value value of the operation
     */
    UfsOps(String value) {
      mValue = value;
    }

    @Override
    public String toString() {
      return mValue;
    }
  }

  // The aggregated metric of an operation targeting a specific UFS
  // triggered by the leading master.
  // TODO(lu) Aggregate the per ufs per op from master + worker + client metrics
  public static final String UFS_OP_PREFIX = "Master.PerUfsOp";
  public static final String UFS_OP_SAVED_PREFIX = "Master.PerUfsSavedOp";

  // Tags
  public static final String TAG_UFS = "UFS";
  public static final String TAG_UFS_TYPE = "UFS_TYPE";
  public static final String TAG_USER = "User";

  // Metric name component
  public static final String TIER = "Tier";

  private MetricInfo() {} // prevent instantiation
}
