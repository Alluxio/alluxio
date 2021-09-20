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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public final class S3RangeSpecTest {
  @Test
  public void invalidRangeSpec() {
    String[] ranges = new String[] {"bytes=100", "bytes=100-1", "bytes=-0"};
    for (String range : ranges) {
      S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
      assertEquals(S3RangeSpec.INVALID_S3_RANGE_SPEC, s3Range);
    }
  }

  @Test
  public void outOfRange() {
    String range = "bytes=100-200";
    long objectSize = 100;
    S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
    assertEquals(0, s3Range.getLength(objectSize));
    assertEquals(0, s3Range.getOffset(objectSize));

    objectSize = 150;
    s3Range = S3RangeSpec.Factory.create(range);
    assertEquals(50, s3Range.getLength(objectSize));
    assertEquals(100, s3Range.getOffset(objectSize));
  }

  @Test
  public void inRange() {
    String range = "bytes=100-200";
    long objectSize = 1000;
    S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
    assertEquals(101, s3Range.getLength(objectSize));
    assertEquals(100, s3Range.getOffset(objectSize));
  }

  @Test
  public void prefixLength() {
    long objectSize = 150;
    String range = "bytes=100-";
    S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
    assertEquals(50, s3Range.getLength(objectSize));
    assertEquals(100, s3Range.getOffset(objectSize));

    range = "bytes=150-";
    s3Range = S3RangeSpec.Factory.create(range);
    assertEquals(0, s3Range.getLength(objectSize));
    assertEquals(0, s3Range.getOffset(objectSize));
  }

  @Test
  public void suffixLength() {
    String range = "bytes=-200";
    long objectSize = 1000;
    S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
    assertEquals(200, s3Range.getLength(objectSize));
    assertEquals(800, s3Range.getOffset(objectSize));

    objectSize = 100;
    s3Range = S3RangeSpec.Factory.create(range);
    assertEquals(100, s3Range.getLength(objectSize));
    assertEquals(0, s3Range.getOffset(objectSize));
  }
}
