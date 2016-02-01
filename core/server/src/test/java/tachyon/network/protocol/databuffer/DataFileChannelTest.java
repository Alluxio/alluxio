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

package tachyon.network.protocol.databuffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;

import tachyon.util.io.BufferUtils;

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
