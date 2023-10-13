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

package alluxio.client.file.cache.cuckoofilter;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class SizeEncoderTest {
  private SizeEncoder mSizeEncoder;
  private static final int PREFIX_BITS = 8;
  private static final int SUFFIX_BITS = 12;

  @Before
  public void beforeTest() {
    mSizeEncoder = new SizeEncoder(PREFIX_BITS + SUFFIX_BITS, PREFIX_BITS);
  }

  @Test
  public void testEncode() {
    int sizeBits = PREFIX_BITS + SUFFIX_BITS;
    for (int i = 0; i < sizeBits; i++) {
      int size = 1 << i;
      int encodedSize = mSizeEncoder.encode(size);
      assertEquals(encodedSize, (size >> SUFFIX_BITS));
    }
  }

  @Test
  public void testDecode() {
    int sizeBits = PREFIX_BITS + SUFFIX_BITS;
    for (int i = SUFFIX_BITS; i < sizeBits; i++) {
      int size = 1 << i;
      mSizeEncoder.add(size);
      int encodedSize = mSizeEncoder.encode(size);
      int decodedSize = mSizeEncoder.dec(encodedSize);
      assertEquals(decodedSize, size);
    }
  }
}
