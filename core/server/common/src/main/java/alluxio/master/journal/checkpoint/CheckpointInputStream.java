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

import alluxio.RuntimeConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
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
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointInputStream.class);
  private final CheckpointType mType;

  /**
   * @param in the underlying stream to read from
   */
  public CheckpointInputStream(InputStream in) throws IOException {
    super(in);
    long id;
    try {
      id = readLong();
    } catch (EOFException e) {
      LOG.error("Failed to read checkpoint type.", e);
      throw new IllegalStateException(String.format(
          "EOF while parsing checkpoint type. Was your checkpoint written by alluxio-1.x? See %s"
              + " for instructions on how to upgrade from alluxio-1.x to alluxio-2.x",
          RuntimeConstants.ALLUXIO_2X_UPGRADE_DOC_URL), e);
    }
    mType = CheckpointType.fromLong(id);
  }

  /**
   * @return the checkpoint type
   */
  public CheckpointType getType() {
    return mType;
  }
}
