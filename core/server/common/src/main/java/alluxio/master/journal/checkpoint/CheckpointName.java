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

package alluxio.master.journal.checkpoint;

/**
 * Names for associating checkpoint data with the classes they represent. To support
 * reading checkpoint written by older versions, these names should never change.
 */
public enum CheckpointName {
  ACTIVE_SYNC_MANAGER,
  BLOCK_MASTER,
  CACHING_INODE_STORE,
  CLUSTER_INFO,
  FILE_SYSTEM_MASTER,
  HEAP_INODE_STORE,
  INODE_COUNTER,
  INODE_DIRECTORY_ID_GENERATOR,
  INODE_TREE,
  MASTER_UFS_MANAGER,
  META_MASTER,
  MOUNT_TABLE,
  NOOP,
  PATH_PROPERTIES,
  PINNED_INODE_FILE_IDS,
  REPLICATION_LIMITED_FILE_IDS,
  ROCKS_INODE_STORE,
  TABLE_MASTER,
  TABLE_MASTER_DATABASE,
  TABLE_MASTER_CATALOG,
  TABLE_MASTER_TRANSFORM_MANAGER,
  TO_BE_PERSISTED_FILE_IDS,
  TTL_BUCKET_LIST,
}
