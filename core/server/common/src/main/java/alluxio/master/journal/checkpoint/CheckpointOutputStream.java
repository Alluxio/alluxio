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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Output stream for writing checkpoints.
 *
 * The stream begins with a type id.
 *
 * @see CheckpointInputStream
 */
public final class CheckpointOutputStream extends DataOutputStream {
  /**
   * @param out the underlying stream to write to
   * @param type the checkpoint type
   */
  public CheckpointOutputStream(OutputStream out, CheckpointType type) throws IOException {
    super(out);
    writeLong(type.getId());
  }
}
