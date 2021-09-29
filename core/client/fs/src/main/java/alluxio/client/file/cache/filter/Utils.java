package alluxio.client.file.cache.filter;

import com.google.common.math.DoubleMath;

import java.math.RoundingMode;

public class Utils {

    public static int indexHash(int hv, int numBuckets) {
        return hv & (numBuckets - 1);
    }

    public static int altIndex(int index, int tag, int numBuckets) {
        return Utils.indexHash((int) (index ^ (tag * 0x5bd1e995)), numBuckets);
    }

    public static int tagHash(int hv, int bitsPerTag) {
        int tag;
        tag = hv & ((1 << bitsPerTag) - 1);
        if (tag == 0) {
            tag++;
        }
        return tag;
    }

    public static IndexAndTag generateIndexAndTag(long hv, int numBuckets, int bitsPerTag) {
        int idx = Utils.indexHash((int) (hv >> 32), numBuckets);
        int tag = Utils.tagHash((int) hv, bitsPerTag);
        return new IndexAndTag(idx, tag);
    }

    public static int optimalBitsPerTag(double fpp, double loadFactor) {
        return DoubleMath.roundToInt(DoubleMath.log2((1 / fpp) + 3) / loadFactor, RoundingMode.UP);
    }

    public static long optimalBuckets(long expectedInsertions, double loadFactor, int tagsPerBucket) {
        long bucketsNeeded = DoubleMath.roundToLong(
                (1.0 / loadFactor) * expectedInsertions / tagsPerBucket, RoundingMode.UP);
        // get next biggest power of 2
        long bitPos = Long.highestOneBit(bucketsNeeded);
        if (bucketsNeeded > bitPos)
            bitPos = bitPos << 1;
        return bitPos;
    }

}
