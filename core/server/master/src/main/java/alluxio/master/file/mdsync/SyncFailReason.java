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

package alluxio.master.file.mdsync;

/**
 * The metadata sync fail reason.
 */
public enum SyncFailReason {
  UNKNOWN,
  UNSUPPORTED,

  LOADING_UFS_IO_FAILURE,
  LOADING_MOUNT_POINT_DOES_NOT_EXIST,

  PROCESSING_UNKNOWN,
  PROCESSING_CONCURRENT_UPDATE_DURING_SYNC,
  PROCESSING_FILE_DOES_NOT_EXIST,
  PROCESSING_MOUNT_POINT_DOES_NOT_EXIST,
}
