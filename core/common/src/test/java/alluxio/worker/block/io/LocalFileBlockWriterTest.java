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

package alluxio.worker.block.io;

import alluxio.exception.status.FailedPreconditionException;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Tests for the {@link LocalFileBlockWriter} class.
 */
public final class LocalFileBlockWriterTest {
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
   */
  @Before
  public void before() throws Exception {
    mTestFilePath = mFolder.newFile().getAbsolutePath();
    mWriter = new LocalFileBlockWriter(mTestFilePath);
  }

  /**
   * Test for the {@link LocalFileBlockWriter#getChannel()} method.
   */
  @Test
  public void getChannel() throws Exception {
    WritableByteChannel channel = mWriter.getChannel();
    Assert.assertNotNull(channel);

    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer((int) TEST_BLOCK_SIZE);
    Assert.assertEquals(TEST_BLOCK_SIZE, channel.write(buffer));
    channel.close();
    Assert.assertEquals(TEST_BLOCK_SIZE, new File(mTestFilePath).length());
  }

  /**
   * Test for the {@link LocalFileBlockWriter#append(ByteBuffer)} method.
   */
  @Test
  public void append() throws Exception {
    ByteBuffer buf = BufferUtils.getIncreasingByteBuffer((int) TEST_BLOCK_SIZE);
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buf));
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buf));
    mWriter.close();
    Assert.assertEquals(2 * TEST_BLOCK_SIZE, new File(mTestFilePath).length());
    // TODO(bin): Read data and assert it is really what we expected using
    // equalIncreasingByteBuffer.
  }

  /**
   * Tests that a {@link FailedPreconditionException} is thrown when trying to append to a channel
   * after closing it.
   */
  @Test
  public void close() throws Exception {
    ByteBuffer buf = BufferUtils.getIncreasingByteBuffer((int) TEST_BLOCK_SIZE);
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buf));
    mWriter.close();
    // Append after close, expect append to fail and throw ClosedChannelException
    mThrown.expect(IOException.class);
    mWriter.append(buf);
  }
}
