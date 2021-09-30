/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.filter;

import java.util.BitSet;

public class BuiltinBitSet extends AbstractBitSet {
  private BitSet mBits;

  public BuiltinBitSet(int nbits) {
    mBits = new BitSet(nbits);
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
