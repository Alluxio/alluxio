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

package tachyon.worker.block.io;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import tachyon.util.io.BufferUtils;

public class LocalFileBlockReaderTest {
  private static final long TEST_BLOCK_SIZE = 1024;
  private LocalFileBlockReader mReader;
  private String mTestFilePath;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mTestFilePath = mFolder.newFile().getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE);
    BufferUtils.writeBufferToFile(mTestFilePath, buffer);
    mReader = new LocalFileBlockReader(mTestFilePath);
  }

  @Test
  public void getChannelTest() throws Exception {
    ReadableByteChannel channel = mReader.getChannel();
    Assert.assertNotNull(channel);
    ByteBuffer buffer = ByteBuffer.allocate((int) TEST_BLOCK_SIZE);
    int bytesRead = channel.read(buffer);
    Assert.assertEquals(TEST_BLOCK_SIZE, bytesRead);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
  }

  @Test
  public void getLengthTest() throws Exception {
    Assert.assertEquals(TEST_BLOCK_SIZE, mReader.getLength());
  }

  @Test
  public void readWithInvalidArgumentTest() throws Exception {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("exceeding fileSize");
    // Read crosses the file length limit
    mReader.read(TEST_BLOCK_SIZE - 1, 2);
  }

  @Test
  public void readTest() throws Exception {
    ByteBuffer buffer;

    // Read 1/4 block by setting the length to be 1/4 of the block size.
    buffer = mReader.read(0, TEST_BLOCK_SIZE / 4);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE / 4, buffer));

    // Read entire block by setting the length to be block size.
    buffer = mReader.read(0, TEST_BLOCK_SIZE);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));

    // Read entire block by setting the length to be -1
    int length = -1;
    buffer = mReader.read(0, length);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
  }

  @Test
  public void closeTest() throws Exception {
    mThrown.expect(ClosedChannelException.class);
    mReader.close();
    // Read after close, expect read to fail and throw ClosedChannelException
    mReader.read(0, TEST_BLOCK_SIZE);
  }
}
