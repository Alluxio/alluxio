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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.exception.status.FailedPreconditionException;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Tests for the {@link LocalFileBlockReader} class.
 */
public class LocalFileBlockReaderTest {
  private static final long TEST_BLOCK_SIZE = 1024;
  private LocalFileBlockReader mReader;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  /**
   * Sets up the file path and file block reader before a test runs.
   */
  @Before
  public void before() throws Exception {
    String testFilePath = mFolder.newFile().getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE);
    BufferUtils.writeBufferToFile(testFilePath, buffer);
    mReader = new LocalFileBlockReader(testFilePath);
  }

  /**
   * Test for the {@link LocalFileBlockReader#getChannel()} method.
   */
  @Test
  public void getChannel() throws Exception {
    ReadableByteChannel channel = mReader.getChannel();
    Assert.assertNotNull(channel);
    ByteBuffer buffer = ByteBuffer.allocate((int) TEST_BLOCK_SIZE);
    int bytesRead = channel.read(buffer);
    Assert.assertEquals(TEST_BLOCK_SIZE, bytesRead);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
  }

  @Test
  public void getLocation() {
    Assert.assertEquals(mReader.getFilePath(), mReader.getLocation());
  }

  /**
   * Test for the {@link LocalFileBlockReader#getLength()} method.
   */
  @Test
  public void getLength() {
    Assert.assertEquals(TEST_BLOCK_SIZE, mReader.getLength());
  }

  /**
   * Tests that an exception is thrown if the read exceeds the file length limit.
   */
  @Test
  public void readWithInvalidArgument() throws Exception {
    Exception e = assertThrows(IllegalArgumentException.class, () -> {
      mReader.read(TEST_BLOCK_SIZE - 1, 2);
    });
    assertTrue(e.getMessage().contains("exceeding fileSize"));
  }

  /**
   * Test for the {@link LocalFileBlockReader#read(long, long)} method.
   */
  @Test
  public void read() throws Exception {
    ByteBuffer buffer;

    // Read 1/4 block by setting the length to be 1/4 of the block size.
    buffer = mReader.read(0, TEST_BLOCK_SIZE / 4);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE / 4, buffer));

    // Read entire block by setting the length to be block size.
    buffer = mReader.read(0, TEST_BLOCK_SIZE);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, (int) TEST_BLOCK_SIZE, buffer));
  }

  /**
   * Tests that a {@link FailedPreconditionException} is thrown when trying to read from a reader
   * after closing it.
   */
  @Test
  public void close() throws Exception {
    mReader.close();
    assertThrows(IOException.class, () -> {
      mReader.read(0, TEST_BLOCK_SIZE);
    });
  }
}
