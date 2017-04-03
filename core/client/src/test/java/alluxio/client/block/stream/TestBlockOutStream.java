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

import alluxio.client.block.BlockWorkerClient;
import alluxio.client.file.options.OutStreamOptions;

import java.nio.ByteBuffer;

import static org.mockito.Mockito.mock;

/**
 * Test class for mocking {@link BlockOutStream} and exposing internal state.
 */
public class TestBlockOutStream extends BlockOutStream {
  private final ByteBuffer mData;
  private boolean mClosed;

  public TestBlockOutStream(ByteBuffer data, long id, long blockSize) {
    super(new TestPacketOutStream(data), id, blockSize, mock(BlockWorkerClient.class),
        OutStreamOptions.defaults());
    mData = data;
    mClosed = false;
  }

  public byte[] getWrittenData() {
    return mData.array();
  }

  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public void close() {
    mClosed = true;
  }
}
