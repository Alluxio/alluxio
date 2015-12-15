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

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.util.io.ByteIOUtils;
import tachyon.worker.keyvalue.Index;
import tachyon.worker.keyvalue.LinearProbingIndex;
import tachyon.worker.keyvalue.PayloadReader;

/**
 * Writer to create a KeyValue file.
 */
public final class KeyValueFileReaderImpl implements KeyValueFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public KeyValueFileReaderImpl(List<Long> blockIds) {
    // For now, only 1 block is allowed.
    Preconditions.checkArgument(blockIds.size() == 1);
  }

  public static Index createIndex(byte[] fileBytes) {
    int length = fileBytes.length;
    int indexOffset = ByteIOUtils.readInt(fileBytes, length - 4);
    // TODO(binfan): this array copy might be expensive and unnecessary
    byte[] indexBytes = Arrays.copyOfRange(fileBytes, indexOffset, length - 4);
    return LinearProbingIndex.loadFromByteArray(indexBytes);
  }

  public static PayloadReader createPayloadReader(byte[] fileBytes) {
    return new PayloadReader(fileBytes);
  }

  @Override
  public byte[] get(byte[] key) {
    LOG.trace("get key");

    return null;
  }

}
