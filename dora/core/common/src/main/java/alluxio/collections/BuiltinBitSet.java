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

package alluxio.collections;

/**
 * This class is a wrapper of java's builtin BitSet.
 */
public class BuiltinBitSet implements BitSet {
  private final java.util.BitSet mBits;

  /**
   * Creates a new bit set. All bits are initially {@code false}.
   *
   * @param nbits the number of bits
   */
  public BuiltinBitSet(int nbits) {
    mBits = new java.util.BitSet(nbits);
  }

  @Override
  public boolean get(int index) {
    return mBits.get(index);
  }

  @Override
  public void set(int index) {
    mBits.set(index);
  }

  @Override
  public void clear(int index) {
    mBits.clear(index);
  }

  @Override
  public int size() {
    return mBits.size();
  }
}
