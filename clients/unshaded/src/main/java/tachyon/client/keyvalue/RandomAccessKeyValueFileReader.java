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

package tachyon.client.keyvalue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.ByteIOUtils;
import tachyon.worker.keyvalue.Index;
import tachyon.worker.keyvalue.LinearProbingIndex;
import tachyon.worker.keyvalue.PayloadReader;
import tachyon.worker.keyvalue.RandomAccessPayloadReader;

/**
 * Reader to access a KeyValue file using random access APIs (e.g., byte[])
 */
public final class RandomAccessKeyValueFileReader implements KeyValueFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private Index mIndex;
  private PayloadReader mPayloadReader;
  private ByteBuffer mBuf;
  private int mBufferLength;

  public RandomAccessKeyValueFileReader(ByteBuffer fileBytes) {
    mBuf = Preconditions.checkNotNull(fileBytes);
    mBufferLength = mBuf.limit();
    mIndex = createIndex();
    mPayloadReader = createPayloadReader();
  }

  public Index createIndex() {
    int indexOffset = ByteIOUtils.readInt(mBuf, mBufferLength - 4);
    ByteBuffer indexBytes =
        BufferUtils.sliceByteBuffer(mBuf, indexOffset, mBufferLength - 4 - indexOffset);
    return LinearProbingIndex.loadFromByteArray(indexBytes);
  }

  public RandomAccessPayloadReader createPayloadReader() {
    return new RandomAccessPayloadReader(mBuf);
  }

  @Override
  public ByteBuffer get(ByteBuffer key) throws IOException {
    LOG.trace("get: key");
    return mIndex.get(key, mPayloadReader);
  }

}
