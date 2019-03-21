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

import alluxio.collections.DelegatingSet;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.master.journal.checkpoint.Checkpointed;
import alluxio.master.journal.checkpoint.LongsCheckpointFormat.LongsCheckpointReader;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A concurrent hash set of Long values that supports writing to and restoring from checkpoints.
 */
@ThreadSafe
public abstract class CheckpointedIdHashSet extends DelegatingSet<Long> implements Checkpointed {
  /**
   * Constructs a new checkpointed id hash set.
   */
  public CheckpointedIdHashSet() {
    super(ConcurrentHashMap.newKeySet());
  }

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException {
    CheckpointOutputStream cos = new CheckpointOutputStream(output, CheckpointType.LONGS);
    for (Long id : this) {
      cos.writeLong(id);
    }
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    clear();
    LongsCheckpointReader reader = new LongsCheckpointReader(input);
    Optional<Long> longOpt;
    while ((longOpt = reader.nextLong()).isPresent()) {
      add(longOpt.get());
    }
  }
}
