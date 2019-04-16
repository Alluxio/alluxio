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

package alluxio.master.file.meta;

import alluxio.master.journal.checkpoint.CheckpointName;

/**
 * Keeps track of which files have limited replication factors.
 */
public class ReplicationLimitedFileIds extends CheckpointedIdHashSet {
  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.REPLICATION_LIMITED_FILE_IDS;
  }
}
