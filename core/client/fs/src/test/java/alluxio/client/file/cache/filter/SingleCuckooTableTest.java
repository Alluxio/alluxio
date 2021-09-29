package alluxio.client.file.cache.filter;

import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SingleCuckooTableTest {
    static final int numBuckets = 16;
    static final int tagsPerBucket = 4;
    static final int bitsPerTag = 8;

    CuckooTable createCuckooTable() {
        AbstractBitSet bits = new BuiltinBitSet(numBuckets * bitsPerTag * 4);
        return new SingleCuckooTable(bits, numBuckets, tagsPerBucket, bitsPerTag);
    }

    @Test
    public void readWriteTagTest() {
        CuckooTable cuckooTable = createCuckooTable();
        Random random = new Random();
        for (int i = 0; i < numBuckets; i++) {
            for (int j = 0; j < tagsPerBucket; j++) {
                int tag = random.nextInt(0xff);
                cuckooTable.writeTag(i, j, tag);
                int t = cuckooTable.readTag(i, j);
                assertEquals(tag, t);
            }
        }
    }

    @Test
    public void findTagTest() {
        CuckooTable cuckooTable = createCuckooTable();
        Random random = new Random();
        for (int i = 0; i < numBuckets; i++) {
            for (int j = 0; j < tagsPerBucket; j++) {
                int tag = random.nextInt(0xff);
                cuckooTable.writeTag(i, j, tag);
                assertTrue(cuckooTable.findTagInBucket(i, tag));
            }
        }
    }

    @Test
    public void deleteTagTest() {
        CuckooTable cuckooTable = createCuckooTable();
        Random random = new Random();
        for (int i = 0; i < numBuckets; i++) {
            for (int j = 0; j < tagsPerBucket; j++) {
                int tag = random.nextInt(0xff);
                cuckooTable.writeTag(i, j, tag);
                assertTrue(cuckooTable.deleteTagFromBucket(i, tag));
            }
        }
    }

    @Test
    public void rewriteTest() {
        CuckooTable cuckooTable = createCuckooTable();
        Random random = new Random();
        for (int i = 0; i < numBuckets; i++) {
            for (int j = 0; j < tagsPerBucket; j++) {
                int tag = random.nextInt(0xff);
                cuckooTable.writeTag(i, j, tag);
                assertTrue(cuckooTable.deleteTagFromBucket(i, tag));
                int tag2 = random.nextInt(0xff);
                cuckooTable.writeTag(i, j, tag2);
                assertTrue(cuckooTable.deleteTagFromBucket(i, tag2));
            }
        }
    }

    @Test
    public void insertOrKickOneTest() {
        CuckooTable cuckooTable = createCuckooTable();
        Random random = new Random();
        for (int i = 0; i < numBuckets; i++) {
            Set<Integer> seen = new HashSet<>();
            for (int j = 0; j < tagsPerBucket; j++) {
                int tag = random.nextInt(0xff);
                cuckooTable.writeTag(i, j, tag);
                seen.add(tag);
            }
            int tag = random.nextInt(0xff);
            int oldTag = cuckooTable.insertOrKickoutOne(i, tag);
            assertTrue(seen.contains(oldTag));
        }
    }
}