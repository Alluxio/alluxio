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

package alluxio.client.file.cache.filter;

import alluxio.Constants;

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
 * A concurrent cuckoo filter with three customized field: clock, size and scope.
 *
 * @param <T> the type of item
 */
public class ConcurrentClockCuckooFilter<T> implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final double DEFAULT_FPP = 0.01;
  private static final double DEFAULT_LOAD_FACTOR = 0.955;
  private static final int MAX_CUCKOO_COUNT = 500;
  private static final int TAGS_PER_BUCKET = 4;
  private static final int DEFAULT_NUM_LOCKS = 4096;
  private static final int MAX_BFS_PATH_LEN = 5;

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

  /**
   * The constructor of concurrent clock cuckoo filter.
   *
   * @param table the table to store fingerprint
   * @param clockTable the table to store clock
   * @param sizeTable the table to store size
   * @param scopeTable the table to store scope
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of sliding window
   * @param funnel the funnel of T's that the constructed cuckoo filter will use
   * @param hasher the hash function the constructed cuckoo filter will use
   */
  public ConcurrentClockCuckooFilter(CuckooTable table, CuckooTable clockTable,
      CuckooTable sizeTable, CuckooTable scopeTable, SlidingWindowType slidingWindowType,
      long windowSize, Funnel<? super T> funnel, HashFunction hasher) {
    mTable = table;
    mNumBuckets = table.getNumBuckets();
    mBitsPerTag = table.getBitsPerTag();
    mClockTable = clockTable;
    mBitsPerClock = clockTable.getBitsPerTag();
    mSizeTable = sizeTable;
    mBitsPerSize = sizeTable.getBitsPerTag();
    mScopeTable = scopeTable;
    mBitsPerScope = scopeTable.getBitsPerTag();
    mSlidingWindowType = slidingWindowType;
    mWindowSize = windowSize;
    mFunnel = funnel;
    mHasher = hasher;
    mScopeEncoder = new ScopeEncoder(mBitsPerScope);
    mLocks = new SegmentedLock(Math.min(DEFAULT_NUM_LOCKS, mNumBuckets >> 1), mNumBuckets);
    // init scope statistics
    int maxNumScopes = (1 << mBitsPerScope);
    mScopeToNumber = new AtomicInteger[maxNumScopes];
    mScopeToSize = new AtomicLong[maxNumScopes];
    for (int i = 0; i < maxNumScopes; i++) {
      mScopeToNumber[i] = new AtomicInteger(0);
      mScopeToSize[i] = new AtomicLong(0);
    }
    // init aging pointers for each lock
    mSegmentedAgingPointers = new int[mLocks.getNumLocks()];
    Arrays.fill(mSegmentedAgingPointers, 0);
  }

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed cuckoo filter will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      BloomFilter}; must be positive
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of the sliding window
   * @param fpp the desired false positive probability (must be positive and less than 1.0)
   * @param loadFactor the load factor of cuckoo filter (must be positive and less than 1.0)
   * @param hasher the hash function to be used
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
  public static <T> ConcurrentClockCuckooFilter<T> create(Funnel<? super T> funnel,
      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
      SlidingWindowType slidingWindowType, long windowSize, double fpp, double loadFactor,
      HashFunction hasher) {
    // make expectedInsertions a power of 2
    int bitsPerTag = Utils.optimalBitsPerTag(fpp, loadFactor);
    long numBuckets = Utils.optimalBuckets(expectedInsertions, loadFactor, TAGS_PER_BUCKET);
    long numBits = numBuckets * TAGS_PER_BUCKET * bitsPerTag;
    // TODO(iluoeli): check numBits overflow (< INT_MAX)
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

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed {@code BloomFilter} will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      BloomFilter}; must be positive
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of the sliding window
   * @param fpp the desired false positive probability (must be positive and less than 1.0)
   * @param loadFactor the load factor of cuckoo filter (must be positive and less than 1.0)
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
  public static <T> ConcurrentClockCuckooFilter<T> create(Funnel<? super T> funnel,
      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
      SlidingWindowType slidingWindowType, long windowSize, double fpp, double loadFactor) {
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        slidingWindowType, windowSize, fpp, loadFactor, Hashing.murmur3_128());
  }

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed {@code BloomFilter} will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      BloomFilter}; must be positive
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of the sliding window
   * @param fpp the desired false positive probability (must be positive and less than 1.0)
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
  public static <T> ConcurrentClockCuckooFilter<T> create(Funnel<? super T> funnel,
      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
      SlidingWindowType slidingWindowType, long windowSize, double fpp) {
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        slidingWindowType, windowSize, fpp, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed {@code BloomFilter} will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      BloomFilter}; must be positive
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of the sliding window
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
  public static <T> ConcurrentClockCuckooFilter<T> create(Funnel<? super T> funnel,
      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
      SlidingWindowType slidingWindowType, long windowSize) {
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        slidingWindowType, windowSize, DEFAULT_FPP);
  }

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed {@code BloomFilter} will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      BloomFilter}; must be positive
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
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

  /**
   * Insert an item into cuckoo filter.
   *
   * @param item the object to be inserted
   * @param size the size of this item
   * @param scopeInfo the scope this item belongs to
   * @return true if inserted successfully; false otherwise
   */
  public boolean put(T item, int size, ScopeInfo scopeInfo) {
    IndexAndTag indexAndTag = generateIndexAndTag(item);
    int fp = indexAndTag.mTag;
    int b1 = indexAndTag.mBucket;
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
    if (done && pos.mStatus == CuckooStatus.OK) {
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

  /**
   * Check whether an item is in cuckoo filter (and reset its clock to MAX) or not.
   *
   * @param item the item to be checked
   * @return true if item is in cuckoo filter; false otherwise
   */
  public boolean mightContainAndResetClock(T item) {
    return mightContainAndOptionalResetClock(item, true);
  }

  /**
   * Check whether an item is in cuckoo filter or not. This method will not change item's clock.
   *
   * @param item the item to be checked
   * @return true if item is in cuckoo filter; false otherwise
   */
  public boolean mightContain(T item) {
    return mightContainAndOptionalResetClock(item, false);
  }

  /**
   * @param item the item to be checked
   * @param shouldReset the flag to indicate whether to reset clock field
   * @return true if item is in cuckoo filter; false otherwise
   */
  private boolean mightContainAndOptionalResetClock(T item, boolean shouldReset) {
    boolean found;
    IndexAndTag indexAndTag = generateIndexAndTag(item);
    int b1 = indexAndTag.mBucket;
    int tag = indexAndTag.mTag;
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

  /**
   * Delete an item from cuckoo filter.
   *
   * @param item the item to be deleted
   * @return true if the item is deleted; false otherwise
   */
  public boolean delete(T item) {
    IndexAndTag indexAndTag = generateIndexAndTag(item);
    int i1 = indexAndTag.mBucket;
    int tag = indexAndTag.mTag;
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
   * A thread-safe method to check aging progress of each segment. Should be called on each T/(2^C),
   * where T is the window size and C is the bits number of the CLOCK field.
   */
  public void aging() {
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

  /**
   * Get the item's clock value (age).
   *
   * @param item the item to be queried
   * @return the clock value
   */
  public int getAge(T item) {
    boolean found;
    IndexAndTag indexAndTag = generateIndexAndTag(item);
    int i1 = indexAndTag.mBucket;
    int tag = indexAndTag.mTag;
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

  /**
   * @return the summary of this cuckoo filter
   */
  public String getSummary() {
    return "numBuckets: " + getNumBuckets() + "\ntagsPerBucket: " + getTagsPerBucket()
        + "\nbitsPerTag: " + getBitsPerTag() + "\nbitsPerClock: " + getBitsPerClock()
        + "\nbitsPerSize: " + mBitsPerSize + "\nbitsPerScope: " + mBitsPerScope + "\nSizeInMB: "
        + (getNumBuckets() * getTagsPerBucket() * getBitsPerTag() / 8.0 / Constants.MB
            + getNumBuckets() * getTagsPerBucket() * getBitsPerClock() / 8.0 / Constants.MB
            + getNumBuckets() * getTagsPerBucket() * mBitsPerSize / 8.0 / Constants.MB
            + getNumBuckets() * getTagsPerBucket() * mBitsPerScope / 8.0 / Constants.MB);
  }

  /**
   * @return the probability that {@linkplain #mightContain(Object)} will erroneously return {@code
   * true} for an object that has not actually been put in the {@code ConcurrentCuckooFilter}.
   */
  public double expectedFpp() {
    // TODO(iluoeli): compute real fpp
    return DEFAULT_FPP;
  }

  /**
   * @return the number of items in this cuckoo filter
   */
  public int getItemNumber() {
    return mNumItems.intValue();
  }

  /**
   * @param scopeInfo the scope bo be queried
   * @return the number of items of specified scope in this cuckoo filter
   */
  public int getItemNumber(ScopeInfo scopeInfo) {
    int scope = encodeScope(scopeInfo);
    return mScopeToNumber[scope].get();
  }

  /**
   * @return the size of items in this cuckoo filter
   */
  public int getItemSize() {
    return mTotalBytes.intValue();
  }

  /**
   * @param scopeInfo the scope bo be queried
   * @return the size of items of specified scope in this cuckoo filter
   */
  public int getItemSize(ScopeInfo scopeInfo) {
    int scope = encodeScope(scopeInfo);
    return mScopeToSize[scope].intValue();
  }

  /**
   * By calling this method, cuckoo filter is informed of the number of entries have passed.
   *
   * @param count the number of operations have passed
   */
  public void increaseOperationCount(int count) {
    mOperationCount.addAndGet(count);
  }

  /**
   * @return the number of buckets this cuckoo filter has
   */
  public int getNumBuckets() {
    return mTable.getNumBuckets();
  }

  /**
   * @return the number of slots per bucket has
   */
  public int getTagsPerBucket() {
    return mTable.getNumTagsPerBuckets();
  }

  /**
   * @return the number of bits per slot has
   */
  public int getBitsPerTag() {
    return mTable.getBitsPerTag();
  }

  /**
   * @return the number of bits per slot's clock field has
   */
  public int getBitsPerClock() {
    return mClockTable.getBitsPerTag();
  }

  /**
   * @return the number of bits per slot's size field has
   */
  public int getBitsPerSize() {
    return mSizeTable.getBitsPerTag();
  }

  /**
   * @return the number of bits per slot's scope field has
   */
  public int getBitsPerScope() {
    return mScopeTable.getBitsPerTag();
  }

  /**
   * Compute the index from a hash value.
   *
   * @param hv the hash value used for computing
   * @return the bucket computed on given hash value
   */
  private int indexHash(int hv) {
    return Utils.indexHash(hv, mNumBuckets);
  }

  /**
   * Compute the tag from a hash value.
   *
   * @param hv the hash value used for computing
   * @return the fingerprint computed on given hash value
   */
  private int tagHash(int hv) {
    return Utils.tagHash(hv, mBitsPerTag);
  }

  /**
   * Compute the alternative index from index and tag.
   *
   * @param index the bucket for computing
   * @param tag the fingerprint for computing
   * @return the alternative bucket
   */
  private int altIndex(int index, int tag) {
    return Utils.altIndex(index, tag, mNumBuckets);
  }

  /**
   * Compute the index and tag for given item.
   *
   * @param item the item for computing
   * @return the bucket and fingerprint of item
   */
  private IndexAndTag generateIndexAndTag(T item) {
    HashCode hashCode = mHasher.newHasher().putObject(item, mFunnel).hash();
    long hv = hashCode.asLong();
    return Utils.generateIndexAndTag(hv, mNumBuckets, mBitsPerTag);
  }

  /**
   * Encode a scope information into a integer type.
   *
   * @param scopeInfo the scope to be encoded
   * @return the encoded number of scope
   */
  private int encodeScope(ScopeInfo scopeInfo) {
    return mScopeEncoder.encode(scopeInfo);
  }

  /**
   * A thread-safe method to update scope statistics.
   *
   * @param scope the scope be be updated
   * @param number the number of items this scope have changed
   * @param size the size of this scope have changed
   */
  private void updateScopeStatistics(int scope, int number, int size) {
    mScopeToNumber[scope].addAndGet(number);
    mScopeToSize[scope].addAndGet(size);
  }

  /**
   * Try find an empty slot for the item. Assume already held the lock of buckets i1 and i2.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @param pos the position
   * @return true iff found an empty slot (stored in pos); false otherwise
   */
  private boolean cuckooInsertLoop(int b1, int b2, int fp, TagPosition pos) {
    int maxRetryNum = 1;
    boolean done = false;
    while (maxRetryNum-- > 0) {
      if (cuckooInsert(b1, b2, fp, pos)) {
        done = true;
        pos.mStatus = CuckooStatus.OK;
        break;
      }
    }
    return done;
  }

  /**
   * Try find an empty slot for the item. Assume already held the lock of buckets i1 and i2.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @param pos the position
   * @return true iff found an empty slot (stored in pos); false otherwise
   */
  private boolean cuckooInsert(int b1, int b2, int fp, TagPosition pos) {
    TagPosition pos1 = new TagPosition();
    TagPosition pos2 = new TagPosition();
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
   * Try find an empty slot for the item. Assume already held the lock of buckets i1 and i2.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @param pos the position
   * @return true iff found an empty slot (stored in pos); false otherwise
   */
  private boolean runCuckoo(int b1, int b2, int fp, TagPosition pos) {
    mLocks.unlockTwoWrite(b1, b2);
    int maxPathLen = MAX_BFS_PATH_LEN;
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
        pos.setBucketAndSlot(cuckooPath[0].mBucket, cuckooPath[0].mSlot);
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

  /**
   * Search for an empty slot from two initial buckets b1 and b2, and move items backwards along the
   * path.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @param cuckooPath the information of searched path
   * @return the depth the path
   */
  private int cuckooPathSearch(int b1, int b2, int fp, CuckooRecord[] cuckooPath) {
    // 1. search a path
    BFSEntry x = slotBFSSearch(b1, b2, fp);
    if (x.mDepth == -1) {
      return -1;
    }
    // 2. re-construct path from x
    for (int i = x.mDepth; i >= 0; i--) {
      cuckooPath[i].mSlot = x.mPathcode % TAGS_PER_BUCKET;
      x.mPathcode /= TAGS_PER_BUCKET;
    }
    if (x.mPathcode == 0) {
      cuckooPath[0].mBucket = b1;
    } else {
      assert x.mPathcode == 1;
      cuckooPath[0].mBucket = b2;
    }
    {
      mLocks.lockOneWrite(cuckooPath[0].mBucket);
      int tag = mTable.readTag(cuckooPath[0].mBucket, cuckooPath[0].mSlot);
      if (tag == 0) {
        mLocks.unlockOneWrite(cuckooPath[0].mBucket);
        return 0;
      }
      mLocks.unlockOneWrite(cuckooPath[0].mBucket);
      cuckooPath[0].mFingerprint = tag;
    }
    for (int i = 1; i <= x.mDepth; i++) {
      CuckooRecord curr = cuckooPath[i];
      CuckooRecord prev = cuckooPath[i - 1];
      curr.mBucket = altIndex(prev.mBucket, prev.mFingerprint);
      mLocks.lockOneWrite(curr.mBucket);
      int tag = mTable.readTag(curr.mBucket, curr.mSlot);
      if (tag == 0) {
        mLocks.unlockOneWrite(curr.mBucket);
        return i;
      }
      curr.mFingerprint = tag;
      mLocks.unlockOneWrite(curr.mBucket);
    }
    return x.mDepth;
  }

  /**
   * Search an empty slot from two initial buckets.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @return the last entry of searched path
   */
  private BFSEntry slotBFSSearch(int b1, int b2, int fp) {
    Queue<BFSEntry> queue = new LinkedList<>();
    queue.offer(new BFSEntry(b1, 0, 0));
    queue.offer(new BFSEntry(b2, 1, 0));
    int maxPathLen = MAX_BFS_PATH_LEN;
    while (!queue.isEmpty()) {
      BFSEntry x = queue.poll();
      mLocks.lockOneWrite(x.mBucket);
      // pick a random slot to start on
      int startingSlot = x.mPathcode % TAGS_PER_BUCKET;
      for (int i = 0; i < TAGS_PER_BUCKET; i++) {
        int slot = (startingSlot + i) % TAGS_PER_BUCKET;
        int tag = mTable.readTag(b1, slot);
        if (tag == 0) {
          x.mPathcode = x.mPathcode * TAGS_PER_BUCKET + slot;
          mLocks.unlockOneWrite(x.mBucket);
          return x;
        }
        if (x.mDepth < maxPathLen - 1) {
          queue.offer(
              new BFSEntry(altIndex(b1, tag), x.mPathcode * TAGS_PER_BUCKET + slot, x.mDepth + 1));
        }
      }
      mLocks.unlockOneWrite(x.mBucket);
    }
    return new BFSEntry(0, 0, -1);
  }

  /**
   * Move items backward along the cuckoo path.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @param cuckooPath the path to move along
   * @param depth the depth of the path
   * @return true if successfully moved items along the path; false otherwise
   */
  private boolean cuckooPathMove(int b1, int b2, int fp, CuckooRecord[] cuckooPath, int depth) {
    if (depth == 0) {
      mLocks.lockTwoWrite(b1, b2);
      if (mTable.readTag(cuckooPath[0].mBucket, cuckooPath[0].mSlot) == 0) {
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
        mLocks.lockThreeWrite(b1, b2, to.mBucket);
      } else {
        mLocks.lockTwoWrite(from.mBucket, to.mBucket);
      }
      int fromTag = mTable.readTag(from.mBucket, from.mSlot);
      // if `to` is nonempty, or `from` is not occupied by original tag,
      // in both cases, abort this insertion.
      if (mTable.readTag(to.mBucket, to.mSlot) != 0 || fromTag != from.mFingerprint) {
        return false;
      }
      mTable.writeTag(to.mBucket, to.mSlot, fromTag);
      mClockTable.writeTag(to.mBucket, to.mSlot, mClockTable.readTag(from.mBucket, from.mSlot));
      mScopeTable.writeTag(to.mBucket, to.mSlot, mScopeTable.readTag(from.mBucket, from.mSlot));
      mSizeTable.writeTag(to.mBucket, to.mSlot, mSizeTable.readTag(from.mBucket, from.mSlot));
      mTable.writeTag(from.mBucket, from.mSlot, 0);
      if (depth == 1) {
        // is it probable to.bucket is one of b1 and b2 ?
        if (to.mBucket != b1 && to.mBucket != b2) {
          mLocks.unlockOneWrite(to.mBucket);
        }
      } else {
        mLocks.unlockTwoWrite(from.mBucket, to.mBucket);
      }
      depth--;
    }
    return true;
  }

  /**
   * Find tag `fp` in bucket b1 and b2.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @return the position of `fp`
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
   * @param i the bucket index
   * @param fp the fingerprint
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
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   */
  private void lockTwoWriteAndOpportunisticAging(int b1, int b2) {
    mLocks.lockTwoWrite(b1, b2);
    opportunisticAgingSegment(mLocks.getSegmentIndex(b1));
    opportunisticAgingSegment(mLocks.getSegmentIndex(b2));
  }

  /**
   * Try opportunistic aging ith segment. Assume holding the lock of this segment.
   *
   * @param i the index of the segment to be aged
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
   *
   * @param i the index of the segment to be aged
   * @param maxAgingNumber the maximum number of buckets to be aged
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
   * @param from the start bucket of the range to be aged
   * @param to the end bucket of the range to be aged
   * @return the number of cleaned buckets
   */
  private int agingRange(int from, int to) {
    int numCleaned = 0;
    for (int i = from; i < to; i++) {
      numCleaned += agingBucket(i);
    }
    return numCleaned;
  }

  /**
   * @param b the bucket to be aged
   * @return the number of cleaned slots
   */
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

  /**
   * A basic entry that records information of a path node during cuckoo BFS search.
   */
  static final class BFSEntry {
    public int mBucket;
    public int mPathcode; // encode slot position of ancestors and it own nodes
    public int mDepth;

    /**
     * @param bucket the bucket of entry
     * @param pathcode the encoded slot position of ancestors and it own
     * @param depth the depth of the entry in path
     */
    BFSEntry(int bucket, int pathcode, int depth) {
      mBucket = bucket;
      mPathcode = pathcode;
      mDepth = depth;
    }
  }

  /**
   * This class represents a detailed cuckoo record, include its position and stored value.
   */
  static final class CuckooRecord {
    public int mBucket;
    public int mSlot;
    public int mFingerprint;

    CuckooRecord() {
      this(-1, -1, 0);
    }

    /**
     * @param bucket the bucket of this record
     * @param slot the slot of this record
     * @param fingerprint the fingerprint of this record
     */
    CuckooRecord(int bucket, int slot, int fingerprint) {
      mBucket = bucket;
      mSlot = slot;
      mFingerprint = fingerprint;
    }
  }
}
