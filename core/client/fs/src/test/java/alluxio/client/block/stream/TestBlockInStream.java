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

package alluxio.client.block.stream;

import alluxio.client.file.options.InStreamOptions;
import alluxio.wire.WorkerNetAddress;

/**
 * Mock of {@link BlockInStream} to create a BlockInStream on a single block. The
 * data of this stream will be read from the given byte array.
 */
public class TestBlockInStream extends BlockInStream {
  /**
   * Constructs a new {@link TestBlockInStream} to be used in tests.
   *
   * @param blockId the id of the block
   * @param data data to read
   */
  public TestBlockInStream(long blockId, byte[] data) {
    super(new TestPacketInStream(data, blockId, data.length, true),
        new WorkerNetAddress().setHost("local"), InStreamOptions.defaults());
  }
}
