package alluxio.client.file.cache.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class UtilsTest {
  static final int numBuckets = 16;
  static final int tagsPerBucket = 4;
  static final int bitsPerTag = 8;

  @Test
  public void testGenerateIndexAndTag() {
    for (int i = 0; i < numBuckets; i++) {
      for (int j = 0; j < tagsPerBucket; j++) {
        IndexAndTag indexAndTag =
            Utils.generateIndexAndTag(i * numBuckets + j, numBuckets, bitsPerTag);
        assertTrue(0 <= indexAndTag.index && indexAndTag.index < numBuckets);
        assertTrue(0 < indexAndTag.tag && indexAndTag.tag <= ((1 << bitsPerTag) - 1));
        int altIndex = Utils.altIndex(indexAndTag.index, indexAndTag.tag, numBuckets);
        int altAltIndex = Utils.altIndex(altIndex, indexAndTag.tag, numBuckets);
        assertEquals(indexAndTag.index, altAltIndex);
      }
    }
  }
}
