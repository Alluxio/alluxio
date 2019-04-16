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

package alluxio.master.journalv0;

import java.io.IOException;

/**
 * This interface enables streaming out to the journal checkpoint.
 */
public interface JournalCheckpointStreamable {
  /**
   * Writes to the journal, in a streaming fashion, via the {@link JournalOutputStream}.
   *
   * @param outputStream the output stream to write to for the journal checkpoint
   */
  void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException;
}
