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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.file.FileOutStream;
import tachyon.worker.keyvalue.Index;
import tachyon.worker.keyvalue.LinearProbingIndex;
import tachyon.worker.keyvalue.PayloadWriter;

/**
 * Writer that creates a Tachyon KeyValue file
 */
public final class KeyValueFileWriterImpl implements KeyValueFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** handler to underline Tachyon file */
  private final FileOutStream mFileOutStream;
  /** number of key-value pairs added */
  private long mKeyCount = 0;
  /** key-value index */
  private Index mIndex;
  /** key-value payload */
  private PayloadWriter mPayloadWriter;

  /**
   * Constructor
   *
   * @param fileOutStream output handler to the key-value file
   */
  public KeyValueFileWriterImpl(FileOutStream fileOutStream) {
    mFileOutStream = Preconditions.checkNotNull(fileOutStream);
    // TODO(binfan): write a header in the file

    // Use linear probing impl of index
    mIndex = LinearProbingIndex.createEmptyIndex();
    mPayloadWriter = new PayloadWriter(mFileOutStream);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(byte[] key, byte[] value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);

    try {
      int offset = mFileOutStream.count();

      // Pack key and value into a byte array payload
      mPayloadWriter.addKeyAndValue(key, value);
      mIndex.put(key, offset);

    } catch (IOException e) {
      LOG.error("failed to add key {}, value {}", key, value);
    }

    mKeyCount ++;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void build() {
    int indexOffset = mFileOutStream.count();
    try {
      mFileOutStream.write(mIndex.toByteArray());
      mFileOutStream.write(indexOffset);
    } catch (IOException e) {
      LOG.error("Failed to dump index");
    }

    try {
      mFileOutStream.close();
    } catch (IOException e) {
      LOG.error("Failed to close KeyValueFileWriter");
    }
  }

  /**
   * @return number of keys
   */
  public long keyCount() {
    return mKeyCount;
  }

  /**
   * @return number of bytes
   */
  public long byteCount() {
    // last pointer to index
    return mFileOutStream.count() + mIndex.size() + Integer.SIZE;
  }
}
