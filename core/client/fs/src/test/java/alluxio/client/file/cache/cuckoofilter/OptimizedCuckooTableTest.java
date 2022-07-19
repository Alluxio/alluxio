package alluxio.client.file.cache.cuckoofilter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.collections.CuckooBitSet;

import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class OptimizedCuckooTableTest {
  private static final int NUM_BUCKETS = 16;
  private static final int TAGS_PER_BUCKET = 4;
  private static final int BITS_PER_TAG = 8;
  OptimizedCuckooTable mOptimizedCuckooTable;

  @Before
  public void setUp() {
    CuckooBitSet bits = new CuckooBitSet(NUM_BUCKETS * BITS_PER_TAG * TAGS_PER_BUCKET);
    mOptimizedCuckooTable = new OptimizedCuckooTable(bits,
        NUM_BUCKETS, TAGS_PER_BUCKET, BITS_PER_TAG);
  }

  @Test
  public void readWriteTagTest() {
    Random random = new Random();
    for (int i = 0; i < NUM_BUCKETS; i++) {
      for (int j = 0; j < TAGS_PER_BUCKET; j++) {
        int tag = random.nextInt(0xff);
        mOptimizedCuckooTable.writeTag(i, j, tag);
        int t = mOptimizedCuckooTable.readTag(i, j);
        assertEquals(tag, t);
      }
    }
  }

  @Test
  public void findTagTest() {
    Random random = new Random();
    for (int i = 0; i < NUM_BUCKETS; i++) {
      for (int j = 0; j < TAGS_PER_BUCKET; j++) {
        int tag = random.nextInt(0xff);
        mOptimizedCuckooTable.writeTag(i, j, tag);
        assertEquals(CuckooStatus.OK, mOptimizedCuckooTable.findTag(i, tag).getStatus());
      }
    }
  }

  @Test
  public void deleteTagTest() {
    Random random = new Random();
    for (int i = 0; i < NUM_BUCKETS; i++) {
      for (int j = 0; j < TAGS_PER_BUCKET; j++) {
        int tag = random.nextInt(0xff);
        mOptimizedCuckooTable.writeTag(i, j, tag);
        assertEquals(CuckooStatus.OK, mOptimizedCuckooTable.deleteTag(i, tag).getStatus());
      }
    }
  }

  @Test
  public void rewriteTest() {
    Random random = new Random();
    for (int i = 0; i < NUM_BUCKETS; i++) {
      for (int j = 0; j < TAGS_PER_BUCKET; j++) {
        int tag = random.nextInt(0xff);
        mOptimizedCuckooTable.writeTag(i, j, tag);
        assertEquals(CuckooStatus.OK, mOptimizedCuckooTable.deleteTag(i, tag).getStatus());
        int tag2 = random.nextInt(0xff);
        mOptimizedCuckooTable.writeTag(i, j, tag2);
        assertEquals(CuckooStatus.OK, mOptimizedCuckooTable.deleteTag(i, tag2).getStatus());
      }
    }
  }

  @Test
  public void insertOrKickOneTest() {
    Random random = new Random();
    for (int i = 0; i < NUM_BUCKETS; i++) {
      Set<Integer> seen = new HashSet<>();
      for (int j = 0; j < TAGS_PER_BUCKET; j++) {
        int tag = random.nextInt(0xff);
        mOptimizedCuckooTable.writeTag(i, j, tag);
        seen.add(tag);
      }
      int tag = random.nextInt(0xff);
      int oldTag = mOptimizedCuckooTable.insertOrKickTag(i, tag);
      assertTrue(seen.contains(oldTag));
    }
  }
}
