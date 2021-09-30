/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.filter;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A basic entry that records information of a path node during Cuckoo BFS search
 */
final class BFSEntry {
  public int bucket;
  public int pathcode; // encode slot position of ancestors and it own nodes
  public int depth;

  BFSEntry(int bucket, int pathcode, int depth) {
    this.bucket = bucket;
    this.pathcode = pathcode;
    this.depth = depth;
  }
}


final class CuckooRecord {
  public int bucket;
  public int slot;
  public int fingerprint;

  CuckooRecord() {
    this(-1, -1, 0);
  }

  CuckooRecord(int bucket, int slot, int fingerprint) {
    this.bucket = bucket;
    this.slot = slot;
    this.fingerprint = fingerprint;
  }
}


public class ConcurrentClockCuckooFilter<T> implements Serializable {
  private static final double DEFAULT_FPP = 0.01;
  private static final double DEFAULT_LOAD_FACTOR = 0.955;
  private static final int MAX_CUCKOO_COUNT = 500;
  private static final int TAGS_PER_BUCKET = 4;
  private static final int DEFAULT_NUM_LOCKS = 4096;

  // aging configurations
  // we do not want to block user operations for too long,
  // so hard limited the aging number of each operation.
  private static final int MAX_AGING_PER_OPERATION = 500;
  private static final int AGING_STEP_SIZE = 5;

  private final AtomicLong mNumItems = new AtomicLong(0);
  private final AtomicLong mTotalBytes = new AtomicLong(0);
  private final AtomicLong mOperationCount = new AtomicLong(0);
  private final AtomicLong mAgingCount = new AtomicLong(0);

  private final AtomicInteger[] mScopeToNumber;
  private final AtomicLong[] mScopeToSize;
  private final int mNumBuckets;
  private final int mBitsPerTag;
  private final int mBitsPerClock;
  private final int mBitsPerSize;
  private final int mBitsPerScope;
  private final Funnel<? super T> mFunnel;
  private final HashFunction mHasher;
  private final ScopeEncoder mScopeEncoder;
  private final SegmentedLock mLocks;
  private final int[] mSegmentedAgingPointers;
  // if count-based sliding window, windowSize is the number of operations;
  // if time-based sliding window, windowSize is the milliseconds in a period.
  private final SlidingWindowType mSlidingWindowType;
  private final long mWindowSize;
  private final long mStartTime = System.currentTimeMillis();
  private CuckooTable mTable;
  private CuckooTable mClockTable;
  private CuckooTable mSizeTable;
  private CuckooTable mScopeTable;

  public ConcurrentClockCuckooFilter(CuckooTable table, CuckooTable clockTable,
      CuckooTable sizeTable, CuckooTable scopeTable, SlidingWindowType slidingWindowType,
      long windowSize, Funnel<? super T> funnel, HashFunction hasher) {
    this.mTable = table;
    this.mNumBuckets = table.numBuckets();
    this.mBitsPerTag = table.bitsPerTag();
    this.mClockTable = clockTable;
    this.mBitsPerClock = clockTable.bitsPerTag();
    this.mSizeTable = sizeTable;
    this.mBitsPerSize = sizeTable.bitsPerTag();
    this.mScopeTable = scopeTable;
    this.mBitsPerScope = scopeTable.bitsPerTag();
    this.mSlidingWindowType = slidingWindowType;
    this.mWindowSize = windowSize;
    this.mFunnel = funnel;
    this.mHasher = hasher;
    this.mScopeEncoder = new ScopeEncoder(mBitsPerScope);
    this.mLocks = new SegmentedLock(Math.min(DEFAULT_NUM_LOCKS, mNumBuckets >> 1), mNumBuckets);
    // init scope statistics
    int maxNumScopes = (1 << mBitsPerScope);
    this.mScopeToNumber = new AtomicInteger[maxNumScopes];
    this.mScopeToSize = new AtomicLong[maxNumScopes];
    for (int i = 0; i < maxNumScopes; i++) {
      this.mScopeToNumber[i] = new AtomicInteger(0);
      this.mScopeToSize[i] = new AtomicLong(0);
    }
    // init aging pointers for each lock
    this.mSegmentedAgingPointers = new int[mLocks.getNumLocks()];
    Arrays.fill(mSegmentedAgingPointers, 0);
  }

  public static <T> ConcurrentClockCuckooFilter<T> create(Funnel<? super T> funnel,
      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
      SlidingWindowType slidingWindowType, long windowSize, double fpp, double loadFactor,
      HashFunction hasher) {
    // TODO: make expectedInsertions a power of 2
    int bitsPerTag = Utils.optimalBitsPerTag(fpp, loadFactor);
    long numBuckets = Utils.optimalBuckets(expectedInsertions, loadFactor, TAGS_PER_BUCKET);
    long numBits = numBuckets * TAGS_PER_BUCKET * bitsPerTag;
    // TODO: check numBits overflow (< INT_MAX)
    AbstractBitSet bits = new BuiltinBitSet((int) numBits);
    CuckooTable table = new SingleCuckooTable(bits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerTag);

    AbstractBitSet clockBits =
        new BuiltinBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerClock));
    CuckooTable clockTable =
        new SingleCuckooTable(clockBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerClock);

    AbstractBitSet sizeBits = new BuiltinBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerSize));
    CuckooTable sizeTable =
        new SingleCuckooTable(sizeBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerSize);

    AbstractBitSet scopeBits =
        new BuiltinBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerScope));
    CuckooTable scopeTable =
        new SingleCuckooTable(scopeBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerScope);
    return new ConcurrentClockCuckooFilter<>(table, clockTable, sizeTable, scopeTable,
        slidingWindowType, windowSize, funnel, hasher);
  }

  public static <T> ConcurrentClockCuckooFilter<T> create(Funnel<? super T> funnel,
      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
      SlidingWindowType slidingWindowType, long windowSize, double fpp, double loadFactor) {
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        slidingWindowType, windowSize, fpp, loadFactor, Hashing.murmur3_128());
  }

  public static <T> ConcurrentClockCuckooFilter<T> create(Funnel<? super T> funnel,
      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
      SlidingWindowType slidingWindowType, long windowSize, double fpp) {
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        slidingWindowType, windowSize, fpp, DEFAULT_LOAD_FACTOR);
  }

  public static <T> ConcurrentClockCuckooFilter<T> create(Funnel<? super T> funnel,
      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
      SlidingWindowType slidingWindowType, long windowSize) {
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        slidingWindowType, windowSize, DEFAULT_FPP);
  }

  public static <T> ConcurrentClockCuckooFilter<T> create(Funnel<? super T> funnel,
      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope) {
    assert funnel != null;
    assert expectedInsertions > 0;
    assert bitsPerClock > 0;
    assert bitsPerSize > 0;
    assert bitsPerScope > 0;
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        SlidingWindowType.NONE, -1, DEFAULT_FPP);
  }

  public boolean put(T item, int size, ScopeInfo scopeInfo) {
    IndexAndTag indexAndTag = generateIndexAndTag(item);
    int fp = indexAndTag.tag;
    int b1 = indexAndTag.index;
    int b2 = altIndex(b1, fp);
    int scope = encodeScope(scopeInfo);
    TagPosition pos = new TagPosition();
    // Generally, we will hold write locks in two places:
    // 1) put/delete;
    // 2) cuckooPathSearch & cuckooPathMove.
    // But We only execute opportunistic aging in put/delete.
    // This is because we expect cuckoo path search & move to be as fast as possible,
    // or it may be more possible to fail.
    lockTwoWriteAndOpportunisticAging(b1, b2);
    boolean done = cuckooInsertLoop(b1, b2, fp, pos);
    if (done && pos.status == CuckooStatus.OK) {
      // b1 and b2 should be insertable for fp, which means:
      // 1. b1 or b2 have at least one empty slot (this is guaranteed until we unlock two buckets);
      // 2. b1 and b2 do not contain duplicated fingerprint.
      assert (pos.getBucketIndex() >= 0 && pos.getTagIndex() >= 0);
      assert mTable.readTag(pos.getBucketIndex(), pos.getTagIndex()) == 0;
      assert !mTable.findTagInBuckets(b1, b2, fp);
      mTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), fp);
      mClockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), 0xffffffff);
      mScopeTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), scope);
      mSizeTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), size);
      // update statistics
      mNumItems.incrementAndGet();
      mTotalBytes.addAndGet(size);
      updateScopeStatistics(scope, 1, size);
      mLocks.unlockTwoWrite(b1, b2);
      return true;
    }
    mLocks.unlockTwoWrite(b1, b2);
    return false;
  }

  public boolean mightContainAndResetClock(T item) {
    return mightContainAndOptionalResetClock(item, true);
  }

  public boolean mightContain(T item) {
    return mightContainAndOptionalResetClock(item, false);
  }

  private boolean mightContainAndOptionalResetClock(T item, boolean shouldReset) {
    boolean found;
    IndexAndTag indexAndTag = generateIndexAndTag(item);
    int b1 = indexAndTag.index;
    int tag = indexAndTag.tag;
    int b2 = altIndex(b1, tag);
    mLocks.lockTwoRead(b1, b2);
    TagPosition pos = new TagPosition();
    found = mTable.findTagInBuckets(b1, b2, tag, pos);
    if (found && shouldReset) {
      // set C to MAX
      mClockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), 0xffffffff);
    }
    mLocks.unlockTwoRead(b1, b2);
    return found;
  }

  public boolean delete(T item) {
    IndexAndTag indexAndTag = generateIndexAndTag(item);
    int i1 = indexAndTag.index;
    int tag = indexAndTag.tag;
    int i2 = altIndex(i1, tag);
    lockTwoWriteAndOpportunisticAging(i1, i2);
    TagPosition pos = new TagPosition();
    if (mTable.deleteTagFromBucket(i1, tag, pos) || mTable.deleteTagFromBucket(i2, tag, pos)) {
      mNumItems.decrementAndGet();
      int scope = mScopeTable.readTag(pos.getBucketIndex(), pos.getTagIndex());
      int size = mSizeTable.readTag(pos.getBucketIndex(), pos.getTagIndex());
      updateScopeStatistics(scope, -1, -size);
      // Clear Clock
      mClockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), 0);
      mLocks.unlockTwoWrite(i1, i2);
      return true;
    }
    mLocks.unlockTwoWrite(i1, i2);
    return false;
  }


  /**
   * Check aging progress of each segment. Should be called on each T/(2^C), where T is the window
   * size and C is the bits number of the CLOCK field.
   */
  public void checkAging() {
    int numSegments = mLocks.getNumLocks();
    int bucketsPerSegment = mLocks.getNumBucketsPerSegment();
    for (int i = 0; i < numSegments; i++) {
      assert mSegmentedAgingPointers[i] <= bucketsPerSegment;
      // TODO(iluoeli): avoid acquire locks here since it may be blocked
      // for a long time if this segment is contended by multiple users.
      mLocks.lockOneSegmentWrite(i);
      if (mSegmentedAgingPointers[i] < bucketsPerSegment) {
        agingSegment(i, bucketsPerSegment);
      }
      mSegmentedAgingPointers[i] = 0;
      mLocks.unlockOneSegmentWrite(i);
    }
  }

  public int aging() {
    int numCleaned = 0;
    for (int i = 0; i < mNumBuckets; i++) {
      numCleaned += agingBucket(i);
    }
    return numCleaned;
  }

  public int getAge(T item) {
    boolean found;
    IndexAndTag indexAndTag = generateIndexAndTag(item);
    int i1 = indexAndTag.index;
    int tag = indexAndTag.tag;
    int i2 = altIndex(i1, tag);
    mLocks.lockTwoRead(i1, i2);
    TagPosition pos = new TagPosition();
    found = mTable.findTagInBuckets(i1, i2, tag, pos);
    if (found) {
      int clock = mClockTable.readTag(pos.getBucketIndex(), pos.getTagIndex());
      mLocks.unlockTwoRead(i1, i2);
      return clock;
    }
    mLocks.unlockTwoRead(i1, i2);
    return 0;
  }

  public String getSummary() {
    return "numBuckets: " + numBuckets() + "\ntagsPerBucket: " + tagsPerBucket() + "\nbitsPerTag: "
        + bitsPerTag() + "\nbitsPerClock: " + getBitsPerClock() + "\nbitsPerSize: " + mBitsPerSize
        + "\nbitsPerScope: " + mBitsPerScope + "\nSizeInMB: "
        + (numBuckets() * tagsPerBucket() * bitsPerTag() / 8.0 / Constants.MB
            + numBuckets() * tagsPerBucket() * getBitsPerClock() / 8.0 / Constants.MB
            + numBuckets() * tagsPerBucket() * mBitsPerSize / 8.0 / Constants.MB
            + numBuckets() * tagsPerBucket() * mBitsPerScope / 8.0 / Constants.MB);
  }

  public double expectedFpp() {
    // TODO(iluoeli): compute real fpp
    return DEFAULT_FPP;
  }

  public int getItemNumber() {
    return mNumItems.intValue();
  }

  public int getItemNumber(ScopeInfo scopeInfo) {
    int scope = encodeScope(scopeInfo);
    return mScopeToNumber[scope].get();
  }

  public int getItemSize() {
    return mTotalBytes.intValue();
  }

  public int getItemSize(ScopeInfo scopeInfo) {
    int scope = encodeScope(scopeInfo);
    return mScopeToSize[scope].intValue();
  }

  /**
   * By calling this method, cuckoo filter is informed of the number of entries have passed.
   */
  public void increaseOperationCount(int count) {
    mOperationCount.addAndGet(count);
  }

  public int numBuckets() {
    return mTable.numBuckets();
  }

  public int tagsPerBucket() {
    return mTable.numTagsPerBuckets();
  }

  public int bitsPerTag() {
    return mTable.bitsPerTag();
  }

  public int getBitsPerClock() {
    return mClockTable.bitsPerTag();
  }

  private int indexHash(int hv) {
    return Utils.indexHash(hv, mNumBuckets);
  }

  private int tagHash(int hv) {
    return Utils.tagHash(hv, mBitsPerTag);
  }

  private int altIndex(int index, int tag) {
    return Utils.altIndex(index, tag, mNumBuckets);
  }

  private IndexAndTag generateIndexAndTag(T item) {
    HashCode hashCode = mHasher.newHasher().putObject(item, mFunnel).hash();
    long hv = hashCode.asLong();
    return Utils.generateIndexAndTag(hv, mNumBuckets, mBitsPerTag);
  }

  private int encodeScope(ScopeInfo scopeInfo) {
    return mScopeEncoder.encode(scopeInfo);
  }

  /**
   * A thread-safe method to update scope statistics.
   */
  private void updateScopeStatistics(int scope, int number, int size) {
    mScopeToNumber[scope].addAndGet(number);
    mScopeToSize[scope].addAndGet(size);
  }

  /**
   * Assume already held the lock of buckets i1 and i2.
   */
  private boolean cuckooInsertLoop(int b1, int b2, int fp, TagPosition pos) {
    int maxRetryNum = 1;
    boolean done = false;
    while (maxRetryNum-- > 0) {
      if (cuckooInsert(b1, b2, fp, pos)) {
        done = true;
        pos.status = CuckooStatus.OK;
        break;
      }
    }
    return done;
  }

  /**
   * Assume already held the lock of buckets i1 and i2.
   */
  private boolean cuckooInsert(int b1, int b2, int fp, TagPosition pos) {
    TagPosition pos1 = new TagPosition(), pos2 = new TagPosition();
    // try find b1 and b2 firstly
    if (!tryFindInsertBucket(b1, fp, pos1)) {
      pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
      return false;
    }
    if (!tryFindInsertBucket(b2, fp, pos2)) {
      pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
      return false;
    }
    if (pos1.getTagIndex() != -1) {
      pos.setBucketAndSlot(b1, pos1.getTagIndex());
      pos.setStatus(CuckooStatus.OK);
      return true;
    }
    if (pos2.getTagIndex() != -1) {
      pos.setBucketAndSlot(b2, pos2.getTagIndex());
      pos.setStatus(CuckooStatus.OK);
      return true;
    }
    // then BFS search from b1 and b2
    boolean done = runCuckoo(b1, b2, fp, pos);
    if (done) {
      // avoid another duplicated key is inserted during runCuckoo.
      if (mTable.findTagInBuckets(b1, b2, fp)) {
        pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
        return false;
      } else {
        return true;
      }
    }
    return false;
  }

  /**
   * Assume already held the lock of buckets i1 and i2.
   *
   * @return true iff find an empty position (stored in pos); false otherwise.
   */
  private boolean runCuckoo(int b1, int b2, int fp, TagPosition pos) {
    mLocks.unlockTwoWrite(b1, b2);
    int maxPathLen = Constants.MAX_BFS_PATH_LEN;
    CuckooRecord[] cuckooPath = new CuckooRecord[maxPathLen];
    for (int i = 0; i < maxPathLen; i++) {
      cuckooPath[i] = new CuckooRecord();
    }
    boolean done = false;
    while (!done) {
      int depth = cuckooPathSearch(b1, b2, fp, cuckooPath);
      if (depth < 0) {
        break;
      }
      if (cuckooPathMove(b1, b2, fp, cuckooPath, depth)) {
        pos.setBucketAndSlot(cuckooPath[0].bucket, cuckooPath[0].slot);
        pos.setStatus(CuckooStatus.OK);
        done = true;
      }
    }
    if (!done) {
      // NOTE: since we assume holding the locks of two buckets before calling this method,
      // we keep this assumptions after return.
      mLocks.lockTwoWrite(b1, b2);
    }
    return done;
  }

  private int cuckooPathSearch(int b1, int b2, int fp, CuckooRecord[] cuckooPath) {
    // 1. search a path
    BFSEntry x = slotBFSSearch(b1, b2, fp);
    if (x.depth == -1) {
      return -1;
    }
    // 2. re-construct path from x
    for (int i = x.depth; i >= 0; i--) {
      cuckooPath[i].slot = x.pathcode % TAGS_PER_BUCKET;
      x.pathcode /= TAGS_PER_BUCKET;
    }
    if (x.pathcode == 0) {
      cuckooPath[0].bucket = b1;
    } else {
      assert x.pathcode == 1;
      cuckooPath[0].bucket = b2;
    }
    {
      mLocks.lockOneWrite(cuckooPath[0].bucket);
      int tag = mTable.readTag(cuckooPath[0].bucket, cuckooPath[0].slot);
      if (tag == 0) {
        mLocks.unlockOneWrite(cuckooPath[0].bucket);
        return 0;
      }
      mLocks.unlockOneWrite(cuckooPath[0].bucket);
      cuckooPath[0].fingerprint = tag;
    }
    for (int i = 1; i <= x.depth; i++) {
      CuckooRecord curr = cuckooPath[i];
      CuckooRecord prev = cuckooPath[i - 1];
      curr.bucket = altIndex(prev.bucket, prev.fingerprint);
      mLocks.lockOneWrite(curr.bucket);
      int tag = mTable.readTag(curr.bucket, curr.slot);
      if (tag == 0) {
        mLocks.unlockOneWrite(curr.bucket);
        return i;
      }
      curr.fingerprint = tag;
      mLocks.unlockOneWrite(curr.bucket);
    }
    return x.depth;
  }

  private BFSEntry slotBFSSearch(int b1, int b2, int fp) {
    Queue<BFSEntry> queue = new LinkedList<>();
    queue.offer(new BFSEntry(b1, 0, 0));
    queue.offer(new BFSEntry(b2, 1, 0));
    int maxPathLen = Constants.MAX_BFS_PATH_LEN;
    while (!queue.isEmpty()) {
      BFSEntry x = queue.poll();
      mLocks.lockOneWrite(x.bucket);
      // pick a random slot to start on
      int startingSlot = x.pathcode % TAGS_PER_BUCKET;
      for (int i = 0; i < TAGS_PER_BUCKET; i++) {
        int slot = (startingSlot + i) % TAGS_PER_BUCKET;
        int tag = mTable.readTag(b1, slot);
        if (tag == 0) {
          x.pathcode = x.pathcode * TAGS_PER_BUCKET + slot;
          mLocks.unlockOneWrite(x.bucket);
          return x;
        }
        if (x.depth < maxPathLen - 1) {
          queue.offer(
              new BFSEntry(altIndex(b1, tag), x.pathcode * TAGS_PER_BUCKET + slot, x.depth + 1));
        }
      }
      mLocks.unlockOneWrite(x.bucket);
    }
    return new BFSEntry(0, 0, -1);
  }

  private boolean cuckooPathMove(int b1, int b2, int fp, CuckooRecord[] cuckooPath, int depth) {
    if (depth == 0) {
      mLocks.lockTwoWrite(b1, b2);
      if (mTable.readTag(cuckooPath[0].bucket, cuckooPath[0].slot) == 0) {
        mLocks.unlockTwoWrite(b1, b2);
        return true;
      } else {
        mLocks.unlockTwoWrite(b1, b2);
        return false;
      }
    }

    while (depth > 0) {
      CuckooRecord from = cuckooPath[depth - 1];
      CuckooRecord to = cuckooPath[depth];
      if (depth == 1) {
        // NOTE: We must hold the locks of b1 and b2.
        // Or their slots may be preempted by another key if we released locks.
        mLocks.lockThreeWrite(b1, b2, to.bucket);
      } else {
        mLocks.lockTwoWrite(from.bucket, to.bucket);
      }
      int fromTag = mTable.readTag(from.bucket, from.slot);
      // if `to` is nonempty, or `from` is not occupied by original tag,
      // in both cases, abort this insertion.
      if (mTable.readTag(to.bucket, to.slot) != 0 || fromTag != from.fingerprint) {
        return false;
      }
      mTable.writeTag(to.bucket, to.slot, fromTag);
      mClockTable.writeTag(to.bucket, to.slot, mClockTable.readTag(from.bucket, from.slot));
      mScopeTable.writeTag(to.bucket, to.slot, mScopeTable.readTag(from.bucket, from.slot));
      mSizeTable.writeTag(to.bucket, to.slot, mSizeTable.readTag(from.bucket, from.slot));
      mTable.writeTag(from.bucket, from.slot, 0);
      if (depth == 1) {
        // is it probable to.bucket is one of b1 and b2 ?
        if (to.bucket != b1 && to.bucket != b2) {
          mLocks.unlockOneWrite(to.bucket);
        }
      } else {
        mLocks.unlockTwoWrite(from.bucket, to.bucket);
      }
      depth--;
    }
    return true;
  }

  /**
   * Find tag `fp` in bucket b1 and b2.
   *
   * @return the position of `fp`.
   */
  private TagPosition cuckooFind(int b1, int b2, int fp) {
    TagPosition pos = new TagPosition(-1, -1);
    if (!tryFindInsertBucket(b1, fp, pos)) {
      return pos;
    }
    if (!tryFindInsertBucket(b2, fp, pos)) {
      return pos;
    }
    pos.setTagIndex(-1);
    return pos;
  }

  /**
   * Find tag `fp` in bucket `i`.
   *
   * @return true if no duplicated key is found, and `pos.slot` points to an empty slot (if pos.tag
   *         != -1); otherwise return false, and store the position of duplicated key in `pos.slot`.
   */
  private boolean tryFindInsertBucket(int i, int fp, TagPosition pos) {
    pos.setBucketAndSlot(i, -1);
    for (int j = 0; j < TAGS_PER_BUCKET; j++) {
      int tag = mTable.readTag(i, j);
      if (tag != 0) {
        if (tag == fp) {
          pos.setTagIndex(j);
          pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
          return false;
        }
      } else {
        pos.setTagIndex(j);
      }
    }
    return true;
  }

  /**
   * Lock two buckets and try opportunistic aging. Since we hold write locks, we can assure that
   * there are no other threads aging the same segment.
   */
  private void lockTwoWriteAndOpportunisticAging(int b1, int b2) {
    mLocks.lockTwoWrite(b1, b2);
    opportunisticAgingSegment(mLocks.getSegmentIndex(b1));
    opportunisticAgingSegment(mLocks.getSegmentIndex(b2));
  }


  /**
   * Try opportunistic aging ith segment. Assume holding the lock of this segment.
   */
  private void opportunisticAgingSegment(int i) {
    int bucketsToAge = computeAgingNumber();
    agingSegment(i, Math.min(bucketsToAge, MAX_AGING_PER_OPERATION));
  }

  /**
   * @Return the number of buckets should be aged.
   */
  private int computeAgingNumber() {
    int bucketsToAge;
    if (mSlidingWindowType == SlidingWindowType.NONE || mWindowSize < 0) {
      bucketsToAge = 0;
    } else if (mSlidingWindowType == SlidingWindowType.COUNT_BASED) {
      bucketsToAge =
          (int) (mNumBuckets * (mOperationCount.doubleValue() / (mWindowSize >> mBitsPerClock))
              - mAgingCount.get());
    } else {
      long elapsedTime = (System.currentTimeMillis() - mStartTime);
      bucketsToAge =
          (int) (mNumBuckets * (elapsedTime / (mWindowSize >> mBitsPerClock)) - mAgingCount.get());
    }
    return bucketsToAge;
  }

  /**
   * Aging the ith segment at most `maxAgingNumber` buckets. Assume holding the lock of this
   * segment.
   */
  private void agingSegment(int i, int maxAgingNumber) {
    int bucketsPerSegment = mLocks.getNumBucketsPerSegment();
    int startPos = mLocks.getSegmentStartPos(i);
    int numAgedBuckets = 0;
    while (numAgedBuckets < maxAgingNumber) {
      int remainingBuckets = bucketsPerSegment - mSegmentedAgingPointers[i];
      assert remainingBuckets >= 0;
      if (remainingBuckets == 0) {
        break;
      }
      // age `AGING_STEP_SIZE` buckets and re-check window border
      int bucketsToAge = Math.min(AGING_STEP_SIZE, remainingBuckets);
      // advance agingCount to inform other threads before real aging
      int from = startPos + mSegmentedAgingPointers[i];
      mAgingCount.addAndGet(bucketsToAge);
      mSegmentedAgingPointers[i] += bucketsToAge;
      agingRange(from, from + bucketsToAge);
      numAgedBuckets += bucketsToAge;
    }
  }

  /**
   * Aging buckets in range [from, to]. Assume holding the locks of this range.
   *
   * @return the number of cleaned buckets.
   */
  private int agingRange(int from, int to) {
    int numCleaned = 0;
    for (int i = from; i < to; i++) {
      numCleaned += agingBucket(i);
    }
    return numCleaned;
  }

  private int agingBucket(int b) {
    int numCleaned = 0;
    for (int j = 0; j < TAGS_PER_BUCKET; j++) {
      int tag = mTable.readTag(b, j);
      if (tag == 0) {
        continue;
      }
      int oldClock = mClockTable.readTag(b, j);
      assert oldClock >= 0;
      if (oldClock > 0) {
        mClockTable.writeTag(b, j, oldClock - 1);
      } else {
        // evict stale item
        numCleaned++;
        mTable.writeTag(b, j, 0);
        mNumItems.decrementAndGet();
        int scope = mScopeTable.readTag(b, j);
        int size = mSizeTable.readTag(b, j);
        updateScopeStatistics(scope, -1, -size);
        mTotalBytes.addAndGet(-size);
      }
    }
    return numCleaned;
  }
}
