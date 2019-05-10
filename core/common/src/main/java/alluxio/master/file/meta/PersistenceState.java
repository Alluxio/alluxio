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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The persistence state of a file in the under-storage system.
 */
@ThreadSafe
public enum PersistenceState {
  NOT_PERSISTED, // file not persisted in the under FS
  TO_BE_PERSISTED, // the file is to be persisted in the under FS
  PERSISTED, // the file is persisted in the under FS
  LOST // the file is lost but not persisted in the under FS
  ;

  /**
   * Encode a state into a single byte.
   *
   * @return the state encoded as a byte
   */
  public byte encode() {
    return (byte) ordinal();
  }

  /**
   * Encode a list of states into a string.
   *
   * @param states The states to encode
   * @return A string where each char represents a state with input ordering preserved
   */
  public static String encode(List<PersistenceState> states) {
    byte[] chars = new byte[states.size()];
    for (int i = 0; i < states.size(); i++) {
      chars[i] = states.get(i).encode();
    }
    return new String(chars);
  }

  /**
   * Decode a string of encoded states back into a list.
   *
   * @param encoded the string with states encoded as chars
   * @return the list of states as PersistenceState values with ordering preserved
   */
  public static List<PersistenceState> decode(String encoded) {
    List<PersistenceState> states = new ArrayList<>(encoded.length());
    for (int i = 0; i < encoded.length(); i++) {
      // use charAt to avoid creating a copy of the underlying byte[]
      states.add(i,  decode((byte) encoded.charAt(i)));
    }
    return states;
  }

  /**
   * Decode a single byte of data into a PersistenceState.
   *
   * @param state the byte to decode
   * @return the PersistenceState which the byte represents
   */
  public static PersistenceState decode(byte state) {
    int loc = state & 0xFF;
    if (loc < PersistenceState.values().length) {
      return PersistenceState.values()[loc];
    } else {
      throw new IllegalArgumentException(
          String.format("Unable to decode PersistenceState. Decoded value is invalid %d", loc));
    }
  }
}
