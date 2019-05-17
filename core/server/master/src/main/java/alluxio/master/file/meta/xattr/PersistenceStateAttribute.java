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

package alluxio.master.file.meta.xattr;

import alluxio.master.file.meta.PersistenceState;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of an extended attribute for {@link PersistenceState}.
 */
public class PersistenceStateAttribute extends AbstractExtendedAttribute<List<PersistenceState>> {

  /** Minimum number of bytes needed to store a persistence state. */
  private static final int ENCODING_SIZE =
      (int) Math.ceil(Math.log((double) PersistenceState.values().length) / Math.log(2.0) / 8.0);

  PersistenceStateAttribute() {
    super(NamespacePrefix.SYSTEM, "ps");
  }

  @Override
  public byte[] encode(List<PersistenceState> states) {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[states.size() * ENCODING_SIZE]);
    int offset = 0;
    for (PersistenceState obj: states) {
      buffer.put(offset, (byte) obj.ordinal());
      offset += ENCODING_SIZE;
    }
    return buffer.array();
  }

  @Override
  public List<PersistenceState> decode(byte[] bytes) {
    Preconditions.checkArgument(bytes.length > 0, String.format("bytes must be at least 1, is %d",
        bytes.length));
    Preconditions.checkArgument(bytes.length % ENCODING_SIZE == 0, String.format("Cannot decode "
        + "persistence state attribute. Byte array is not a multiple of encoding size. Got %d, "
        + "must be a multiple of %d.", bytes.length, ENCODING_SIZE));

    int numObjects = bytes.length / ENCODING_SIZE;
    ArrayList<PersistenceState> obj = new ArrayList<>(numObjects);
    int loc;
    for (int i = 0; i < numObjects; i++) {
      loc = bytes[i] & 0xFF;
      obj.add(PersistenceState.values()[loc]);
    }
    return obj;
  }
}
