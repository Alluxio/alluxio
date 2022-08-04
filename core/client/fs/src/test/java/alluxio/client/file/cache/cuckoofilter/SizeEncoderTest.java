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
  private int mPrefixBits;
  private int mSuffixBits;

  @Before
  public void beforeTest() {
    mPrefixBits = 8;
    mSuffixBits = 12;
    mSizeEncoder = new SizeEncoder(mPrefixBits + mSuffixBits, mPrefixBits);
  }

  @Test
  public void testEncode() {
    int sizeBits = mPrefixBits + mSuffixBits;
    for (int i = 0; i < sizeBits; i++) {
      int size = 1 << i;
      int encodedSize = mSizeEncoder.encode(size);
      assertEquals(encodedSize, (size >> mSuffixBits));
    }
  }

  @Test
  public void testDecode() {
    int sizeBits = mPrefixBits + mSuffixBits;
    for (int i = mSuffixBits; i < sizeBits; i++) {
      int size = 1 << i;
      mSizeEncoder.add(size);
      int encodedSize = mSizeEncoder.encode(size);
      int decodedSize = mSizeEncoder.dec(encodedSize);
      assertEquals(decodedSize, size);
    }
  }
}
