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

package alluxio.fuse.file;

import alluxio.Constants;
import alluxio.client.file.MockFileInStream;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.wire.FileInfo;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class MemoryCacheFileInStreamTest {

  private final Random mRandom = new Random();

  @Test
  public void testRead() throws IOException, AlluxioException {
    for (int i = 0; i < 10; i++) {
      int size = 1 + mRandom.nextInt(16 * Constants.MB);
      byte[] bytes = new byte[size];
      mRandom.nextBytes(bytes);
      try (MemoryCacheFileInStream memoryCacheFileInStream = createRandomMemoryCacheFileInStream(
          bytes);
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        IOUtils.copy(memoryCacheFileInStream, outputStream, 1024 + mRandom.nextInt(4096));
        Assert.assertArrayEquals(bytes, outputStream.toByteArray());
        MemoryCacheFileInStream.invalidateAllCache();
      }
    }
  }

  private MemoryCacheFileInStream createRandomMemoryCacheFileInStream(byte[] bytes)
      throws IOException, AlluxioException {
    MockFileInStream fileInStream = new MockFileInStream(bytes);
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath(UUID.randomUUID().toString());
    fileInfo.setLastModificationTimeMs(Math.abs(mRandom.nextLong()));
    fileInfo.setLength(bytes.length);
    return new MemoryCacheFileInStream(new URIStatus(fileInfo), fileInStream);
  }
}
