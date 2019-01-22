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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Test class for mocking {@link BlockOutStream} and exposing internal state.
 */
public class TestBlockOutStream extends BlockOutStream {
  private final ByteBuffer mData;
  private boolean mClosed;
  private boolean mCanceled;

  /**
   * Constructs a new {@link TestBlockOutStream} to be used in tests.
   *
   * @param data the data to test
   * @param blockSize the block size
   */
  public TestBlockOutStream(ByteBuffer data, long blockSize) {
    super(new TestDataWriter(data), blockSize, new WorkerNetAddress());
    mData = data;
    mClosed = false;
    mCanceled = false;
  }

  public byte[] getWrittenData() {
    try {
      super.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Arrays.copyOfRange(mData.array(), 0, mData.position());
  }

  public boolean isClosed() {
    return mClosed;
  }

  public boolean isCanceled() {
    return mCanceled;
  }

  @Override
  public void close() throws IOException {
    super.close();
    mClosed = true;
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    super.cancel();
    mCanceled = true;
    mClosed = true;
  }
}
