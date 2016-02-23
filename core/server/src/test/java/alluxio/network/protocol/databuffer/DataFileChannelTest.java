/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.network.protocol.databuffer;

import alluxio.util.io.BufferUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Tests for the {@link DataFileChannel} class.
 */
public class DataFileChannelTest {
  private static final int OFFSET = 1;
  private static final int LENGTH = 5;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private FileInputStream mInputStream = null;
  private FileChannel mChannel = null;

  /**
   * Sets up the dependencies before a test runs.
   *
   * @throws IOException if setting up a dependency fails
   */
  @Before
  public final void before() throws IOException {
    // Create a temporary file for the FileChannel.
    File f = mFolder.newFile("temp.txt");
    String path = f.getAbsolutePath();

    FileOutputStream os = new FileOutputStream(path);
    os.write(BufferUtils.getIncreasingByteArray(OFFSET + LENGTH));
    os.close();

    mInputStream = new FileInputStream(f);
    mChannel = mInputStream.getChannel();
  }

  /**
   * Closes the stream after a test ran.
   *
   * @throws IOException if closing the stream fails
   */
  @After
  public final void after() throws IOException {
    mInputStream.close();
  }

  /**
   * Tests the {@link DataFileChannel#getNettyOutput()} method.
   */
  @Test
  public void nettyOutputTest() {
    DataFileChannel data = new DataFileChannel(mChannel, OFFSET, LENGTH);
    Object output = data.getNettyOutput();
    Assert.assertTrue(output instanceof ByteBuf || output instanceof FileRegion);
  }

  /**
   * Tests the {@link DataFileChannel#getLength()} method.
   */
  @Test
  public void lengthTest() {
    DataFileChannel data = new DataFileChannel(mChannel, OFFSET, LENGTH);
    Assert.assertEquals(LENGTH, data.getLength());
  }

  /**
   * Tests the {@link DataFileChannel#getReadOnlyByteBuffer()} method.
   */
  @Test
  public void readOnlyByteBufferTest() {
    DataFileChannel data = new DataFileChannel(mChannel, OFFSET, LENGTH);
    ByteBuffer readOnlyBuffer = data.getReadOnlyByteBuffer();
    Assert.assertTrue(readOnlyBuffer.isReadOnly());
    Assert.assertEquals(BufferUtils.getIncreasingByteBuffer(OFFSET, LENGTH), readOnlyBuffer);
  }
}
