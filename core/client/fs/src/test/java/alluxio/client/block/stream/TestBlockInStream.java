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

import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link BlockInStream} which reads from the given byte array. The stream is able to track how
 * much bytes that have been read from the extended BlockInStream.
 */
public class TestBlockInStream extends BlockInStream {
  /** A field tracks how much bytes read. */
  private int mBytesRead;
  private boolean mClosed;

  public TestBlockInStream(byte[] data, long id, long length, boolean shortCircuit,
      BlockInStreamSource source) {
    super(new Factory(data, shortCircuit),
        new InstancedConfiguration(ConfigurationUtils.defaults()),
        new WorkerNetAddress(), source, id, length);
    mBytesRead = 0;
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    int bytesRead = super.read(byteBuffer, off, len);
    if (bytesRead <= 0) {
      return bytesRead;
    }
    mBytesRead += bytesRead;
    return bytesRead;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    int bytesRead = super.positionedRead(pos, b, off, len);
    if (bytesRead <= 0) {
      return bytesRead;
    }
    mBytesRead += bytesRead;
    return bytesRead;
  }

  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public void close() throws IOException {
    mClosed = true;
    super.close();
  }

  /**
   * @return how many bytes been read
   */
  public int getBytesRead() {
    return mBytesRead;
  }

  /**
   * Factory class to create {@link TestDataReader}s.
   */
  public static class Factory implements DataReader.Factory {
    private final byte[] mData;
    private final boolean mShortCircuit;

    /**
     * Creates an instance of {@link LocalFileDataReader.Factory}.
     *
     * @param data the data to serve
     */
    public Factory(byte[] data, boolean shortCircuit) {
      mData = data;
      mShortCircuit = shortCircuit;
    }

    @Override
    public DataReader create(long offset, long len) {
      return new TestDataReader(mData, 128, offset, len);
    }

    @Override
    public void close() {}
  }
}
