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

package alluxio.proxy.s3;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class RateLimitInputStreamTest {

  public static final int MB = 1024 * 1024;
  public static final int KB = 1024;

  private byte[] mData;

  @Before
  public void init() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(MB);
    int count = 0;
    while (count < MB) {
      byte[] bytes = UUID.randomUUID().toString().getBytes();
      byteArrayOutputStream.write(bytes);
      count += bytes.length;
    }
    mData = Arrays.copyOf(byteArrayOutputStream.toByteArray(), MB);
  }

  @Test
  public void testRead() throws IOException {
    long rate = 100 * KB;
    ByteArrayInputStream inputStream = new ByteArrayInputStream(mData);
    RateLimitInputStream rateLimitInputStream = new RateLimitInputStream(inputStream, rate);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(MB);
    long start = System.currentTimeMillis();
    IOUtils.copy(rateLimitInputStream, byteArrayOutputStream, KB);
    long end = System.currentTimeMillis();
    long duration = end - start;
    long expectedDuration = MB / rate * 1000;
    Assert.assertTrue(duration >= expectedDuration && duration <= expectedDuration + 1000);
    Assert.assertArrayEquals(mData, byteArrayOutputStream.toByteArray());
  }
}
