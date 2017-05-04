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

package alluxio.network.protocol.databuffer;

import alluxio.util.io.BufferUtils;

import io.netty.channel.FileRegion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Tests for the {@link DataFileChannel} class.
 */
public class DataFileChannelTest {
  private static final int OFFSET = 1;
  private static final int LENGTH = 5;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private File mFile;

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public final void before() throws IOException {
    // Create a temporary file for the FileChannel.
    mFile = mFolder.newFile("temp.txt");
    String filePath = mFile.getAbsolutePath();

    FileOutputStream os = new FileOutputStream(filePath);
    os.write(BufferUtils.getIncreasingByteArray(OFFSET + LENGTH));
    os.close();
  }

  /**
   * Tests the {@link DataFileChannel#getNettyOutput()} method.
   */
  @Test
  public void nettyOutput() {
    DataFileChannel data = new DataFileChannel(mFile, OFFSET, LENGTH);
    Object output = data.getNettyOutput();
    Assert.assertTrue(output instanceof FileRegion);
  }

  /**
   * Tests the {@link DataFileChannel#getLength()} method.
   */
  @Test
  public void length() {
    DataFileChannel data = new DataFileChannel(mFile, OFFSET, LENGTH);
    Assert.assertEquals(LENGTH, data.getLength());
  }
}
