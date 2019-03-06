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

package alluxio.master.journal;

/**
 * Names for associating checkpoint data with the classes they represent. To support
 * reading checkpoint written by older versions, these names should never change.
 */
public enum CheckpointName {
  ACTIVE_SYNC_MANAGER,
  BLOCK_MASTER,
  FILE_SYSTEM_MASTER,
  INODE_DIRECTORY_ID_GENERATOR,
  INODE_TREE,
  MASTER_UFS_MANAGER,
  MOUNT_TABLE,
  NOOP,
}
