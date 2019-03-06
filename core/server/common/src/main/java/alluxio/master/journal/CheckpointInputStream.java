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

import alluxio.master.CheckpointType;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Input stream for reading checkpoints.
 *
 * The stream reads the checkpoint version number.
 *
 * @see CheckpointOutputStream
 */
public final class CheckpointInputStream extends DataInputStream {
  private final CheckpointType mType;

  /**
   * @param in the underlying stream to read from
   */
  public CheckpointInputStream(InputStream in) throws IOException {
    super(in);
    mType = CheckpointType.fromLong(readLong());
  }

  /**
   * @return the checkpoint type
   */
  public CheckpointType getType() {
    return mType;
  }
}
