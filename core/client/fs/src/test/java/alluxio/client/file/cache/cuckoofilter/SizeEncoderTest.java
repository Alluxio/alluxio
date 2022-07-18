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
