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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import tachyon.util.io.BufferUtils;

/**
 * Tests for the {@link LocalFileBlockWriter} class.
 */
public class LocalFileBlockWriterTest {
  private static final long TEST_BLOCK_SIZE = 1024;

  private LocalFileBlockWriter mWriter;
  private String mTestFilePath;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the file path and writer before a test runs.
   *
   * @throws Exception if one of the file operations fails
   */
  @Before
  public void before() throws Exception {
    mTestFilePath = mFolder.newFile().getAbsolutePath();
    mWriter = new LocalFileBlockWriter(mTestFilePath);
  }

  /**
   * Test for the {@link LocalFileBlockWriter#getChannel()} method.
   *
   * @throws Exception if writing to the channel or closing it fails
   */
  @Test
  public void getChannelTest() throws Exception {
    WritableByteChannel channel = mWriter.getChannel();
    Assert.assertNotNull(channel);

    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer((int) TEST_BLOCK_SIZE);
    Assert.assertEquals(TEST_BLOCK_SIZE, channel.write(buffer));
    channel.close();
    Assert.assertEquals(TEST_BLOCK_SIZE, new File(mTestFilePath).length());
  }

  /**
   * Test for the {@link LocalFileBlockWriter#append(ByteBuffer)} method.
   *
   * @throws Exception if appending to the channel or closing it fails
   */
  @Test
  public void appendTest() throws Exception {
    ByteBuffer buf = BufferUtils.getIncreasingByteBuffer((int) TEST_BLOCK_SIZE);
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buf));
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buf));
    mWriter.close();
    Assert.assertEquals(2 * TEST_BLOCK_SIZE, new File(mTestFilePath).length());
    // TODO(bin): Read data and assert it is really what we expected using
    // equalIncreasingByteBuffer.
  }

  /**
   * Tests that a {@link ClosedChannelException} is thrown when trying to append to a channel after
   * closing it.
   *
   * @throws Exception if appending to the channel or closing it fails
   */
  @Test
  public void closeTest() throws Exception {
    mThrown.expect(ClosedChannelException.class);

    ByteBuffer buf = BufferUtils.getIncreasingByteBuffer((int) TEST_BLOCK_SIZE);
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buf));
    mWriter.close();
    // Append after close, expect append to fail and throw ClosedChannelException
    mWriter.append(buf);
  }
}
