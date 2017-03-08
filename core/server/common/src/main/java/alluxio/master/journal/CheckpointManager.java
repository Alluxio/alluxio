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

import java.net.URI;

/**
 * Interface for managing the checkpoint for a journal. The {@link #update(URI)} method will
 * update the journal's checkpoint to the specified location, and
 * {@link #recover()} will recover from any failures that may occur during {@link #update(URI)}.
 */
public interface CheckpointManager {

  /**
   * Recovers the checkpoint in case the master crashed while updating it.
   */
  void recover();

  /**
   * Updates the checkpoint to the specified URI.
   *
   * @param location the location of the new checkpoint
   */
  void update(URI location);
}
