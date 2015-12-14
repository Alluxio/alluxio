/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.keyvalue;

import com.google.common.base.Preconditions;
import tachyon.util.io.ByteIOUtils;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Reader to access key value pairs in payload
 */
public final class PayloadReader {
  private byte[] mBuf;

  public PayloadReader(byte[] buf) {
    mBuf = Preconditions.checkNotNull(buf);
  }

  public byte[] getKey(int pos) {
    final int keyLength = ByteIOUtils.readInt(mBuf, pos);
    final int keyFrom = pos + 8;
    return Arrays.copyOfRange(mBuf, keyFrom, keyFrom + keyLength);
  }

  public byte[] getValue(int pos) {
    final int keyLength = ByteIOUtils.readInt(mBuf, pos);
    final int valueLength = ByteIOUtils.readInt(mBuf, pos + 4);
    final int valueFrom = pos + 8 + keyLength;
    return Arrays.copyOfRange(mBuf, valueFrom, valueFrom + valueLength);
  }

}
