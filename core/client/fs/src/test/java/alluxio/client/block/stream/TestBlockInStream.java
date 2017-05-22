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

import alluxio.wire.WorkerNetAddress;

/**
 * A {@link BlockInStream} which reads from the given byte array.
 */
public class TestBlockInStream extends BlockInStream {
  public TestBlockInStream(byte[] mData, long id, long length, boolean shortCircuit) {
    super(new Factory(mData, shortCircuit), new WorkerNetAddress().setHost("local"), id, length);
  }

  /**
   * Factory class to create {@link TestPacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private final byte[] mData;
    private final boolean mShortCircuit;

    /**
     * Creates an instance of {@link LocalFilePacketReader.Factory}.
     *
     * @param data the data to serve
     */
    public Factory(byte[] data, boolean shortCircuit) {
      mData = data;
      mShortCircuit = shortCircuit;
    }

    @Override
    public PacketReader create(long offset, long len) {
      return new TestPacketReader(mData, offset, len);
    }

    @Override
    public boolean isShortCircuit() {
      return mShortCircuit;
    }

    @Override
    public void close() {}
  }
}
