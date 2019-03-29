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

package alluxio.master;

import alluxio.RuntimeConstants;

/**
 * Enumeration of different checkpoint types used by the master.
 */
public enum CheckpointType {
  /**
   * This format sequentially writes delimited journal entries one after another using
   * the protocol buffer writeDelimitedTo method.
   */
  JOURNAL_ENTRY(0),
  /**
   * This format uses kryo's chunked encoding to write multiple [checkpoint_name, checkpoint_bytes]
   * entries to the output stream.
   */
  COMPOUND(1);

  private final long mId;

  CheckpointType(long id) {
    mId = id;
  }

  /**
   * @return the checkpoint type's id
   */
  public long getId() {
    return mId;
  }

  /**
   * @param id a checkpoint type id
   * @return the corresponding checkpoint type
   */
  public static CheckpointType fromLong(long id) {
    for (CheckpointType type : values()) {
      if (type.getId() == id) {
        return type;
      }
    }
    throw new IllegalStateException(String.format("Unknown checkpoint type id: %d. Was your "
        + "checkpoint written by alluxio-1.x? See %s for instructions on how to upgrade from "
        + "alluxio-1.x to alluxio-2.x", id, RuntimeConstants.ALLUXIO_2X_UPGRADE_DOC_URL));
  }
}
