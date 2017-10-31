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

import alluxio.util.io.BufferUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Tests for the {@link LocalFileBlockWriter} class.
 */
public final class LocalFileBlockWriterTest {
  private static final int TEST_BLOCK_SIZE = 1024;

  private LocalFileBlockWriter mWriter;
  private String mTestFilePath;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mTestFilePath = mFolder.newFile().getAbsolutePath();
    mWriter = new LocalFileBlockWriter(mTestFilePath);
  }

  @After
  public void after() throws Exception {
    mWriter.close();
  }

  @Test
  public void appendByteBuf() throws Exception {
    ByteBuf buffer = Unpooled.wrappedBuffer(
        BufferUtils.getIncreasingByteBuffer(TEST_BLOCK_SIZE));
    buffer.markReaderIndex();
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buffer));
    buffer.resetReaderIndex();
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buffer));
    mWriter.close();
    Assert.assertEquals(2 * TEST_BLOCK_SIZE, new File(mTestFilePath).length());
    ByteBuffer result = ByteBuffer.wrap(Files.readAllBytes(Paths.get(mTestFilePath)));
    result.position(0).limit(TEST_BLOCK_SIZE);
    BufferUtils.equalIncreasingByteBuffer(0, TEST_BLOCK_SIZE, result.slice());
    result.position(TEST_BLOCK_SIZE).limit(TEST_BLOCK_SIZE * 2);
    BufferUtils.equalIncreasingByteBuffer(0, TEST_BLOCK_SIZE, result.slice());
  }

  @Test
  public void append() throws Exception {
    ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(TEST_BLOCK_SIZE);
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buf));
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buf));
    mWriter.close();
    Assert.assertEquals(2 * TEST_BLOCK_SIZE, new File(mTestFilePath).length());
    ByteBuffer result = ByteBuffer.wrap(Files.readAllBytes(Paths.get(mTestFilePath)));
    result.position(0).limit(TEST_BLOCK_SIZE);
    BufferUtils.equalIncreasingByteBuffer(0, TEST_BLOCK_SIZE, result.slice());
    result.position(TEST_BLOCK_SIZE).limit(TEST_BLOCK_SIZE * 2);
    BufferUtils.equalIncreasingByteBuffer(0, TEST_BLOCK_SIZE, result.slice());
  }

  @Test
  public void close() throws Exception {
    ByteBuffer buf = BufferUtils.getIncreasingByteBuffer(TEST_BLOCK_SIZE);
    Assert.assertEquals(TEST_BLOCK_SIZE, mWriter.append(buf));
    mWriter.close();
    // Append after close, expect append to fail and throw ClosedChannelException
    mThrown.expect(IOException.class);
    mWriter.append(buf);
  }
}
