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

package alluxio.client.block;

import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Tests for the {@link UnderStoreBlockInStream} class.
 */
public class UnderStoreBlockInStreamTest {
  private static final long BLOCK_LENGTH = 100L;
  private static final long FILE_LENGTH = 2 * BLOCK_LENGTH;
  private static final String TEST_FILENAME = "test_filename.txt";

  /** Stream for the first block. */
  private UnderStoreBlockInStream mBlockStream;
  /** Stream for the last block. */
  private UnderStoreBlockInStream mEOFBlockStream;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  /**
   * Sets up the streams before a test runs.
   */
  @Before
  public void before() throws IOException {
    File file = mFolder.newFile(TEST_FILENAME);
    FileOutputStream os = new FileOutputStream(file);
    // Create a file of 2 block sizes.
    os.write(BufferUtils.getIncreasingByteArray((int) FILE_LENGTH));
    os.close();
    mBlockStream = new UnderStoreBlockInStream(0, BLOCK_LENGTH, file.getAbsolutePath());
    mEOFBlockStream =
        new UnderStoreBlockInStream(BLOCK_LENGTH, BLOCK_LENGTH, file.getAbsolutePath());
  }

  /**
   * Verifies the byte-by-byte read returns the correct data, for the first block in the file.
   *
   * @throws IOException when reading from the stream fails
   */
  @Test
  public void singleByteReadTest() throws IOException {
    singleByteReadInternal(mBlockStream, 0);
  }

  /**
   * Verifies the byte-by-byte read returns the correct data, for the last block in the file.
   *
   * @throws IOException when reading from the stream fails
   */
  @Test
  public void singleByteReadEOFTest() throws IOException {
    singleByteReadInternal(mEOFBlockStream, (int) BLOCK_LENGTH);
  }

  /**
   * Internal test case to verify byte-by-byte reading of an in stream.
   *
   * @param inStream the stream to read from
   * @param startIndex the start index of the file to read from
   * @throws IOException when reading from the stream fails
   */
  private void singleByteReadInternal(UnderStoreBlockInStream inStream, int startIndex)
      throws IOException {
    long remaining = mBlockStream.remaining();
    for (int i = startIndex; i < startIndex + BLOCK_LENGTH; i++) {
      Assert.assertEquals(i, inStream.read());
      remaining--;
      Assert.assertEquals(remaining, inStream.remaining());
    }
    // Block in stream should be complete, and should return -1, not real data.
    Assert.assertEquals(-1, inStream.read());
    Assert.assertEquals(0, inStream.remaining());
  }

  /**
   * Tests that array read methods read the correct data, for the first block of the file.
   *
   * @throws IOException when reading from the stream fails
   */
  @Test
  public void arrayReadTest() throws IOException {
    arrayReadInternal(mBlockStream, 0);
  }

  /**
   * Tests that array read methods read the correct data, for the last block of the file.
   *
   * @throws IOException when reading from the stream fails
   */
  @Test
  public void arrayReadEOFTest() throws IOException {
    arrayReadInternal(mEOFBlockStream, (int) BLOCK_LENGTH);
  }

  /**
   * Internal test case to verify array read methods an in stream.
   *
   * @param inStream the stream to read from
   * @param startIndex the start index of the file to read from
   * @throws IOException when reading from the stream fails
   */
  private void arrayReadInternal(UnderStoreBlockInStream inStream, int startIndex)
      throws IOException {
    long remaining = inStream.remaining();
    int size = (int) BLOCK_LENGTH / 10;
    byte[] readBytes = new byte[size];

    // Read first 10 bytes
    Assert.assertEquals(size, inStream.read(readBytes));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(startIndex + 0, size, readBytes));
    remaining -= 10;
    Assert.assertEquals(remaining, inStream.remaining());

    // Read next 10 bytes
    Assert.assertEquals(size, inStream.read(readBytes));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(startIndex + size, size, readBytes));
    remaining -= 10;
    Assert.assertEquals(remaining, inStream.remaining());

    // Read with offset and length
    Assert.assertEquals(1, inStream.read(readBytes, size - 1, 1));
    Assert.assertEquals(startIndex + size * 2, readBytes[size - 1]);
    remaining--;
    Assert.assertEquals(remaining, inStream.remaining());
  }
}
