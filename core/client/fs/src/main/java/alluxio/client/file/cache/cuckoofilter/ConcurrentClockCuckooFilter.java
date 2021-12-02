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

import alluxio.Constants;
import alluxio.client.quota.CacheScope;
import alluxio.collections.BitSet;
import alluxio.collections.BuiltinBitSet;

import com.google.common.base.Preconditions;
import com.google.common.hash.Funnel;
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
public class ConcurrentClockCuckooFilter<T> implements ClockCuckooFilter<T>, Serializable {
  public static final double DEFAULT_FPP = 0.01;
  // The default load factor is from "Cuckoo Filter: Practically Better Than Bloom" by Fan et al.
  public static final double DEFAULT_LOAD_FACTOR = 0.955;
  public static final int TAGS_PER_BUCKET = 4;

  private static final long serialVersionUID = 1L;

  // zero tag means non-existed
  private static final int NON_EXISTENT_TAG = 0;

  private static final int DEFAULT_NUM_LOCKS = 4096;

  // the maximum number of entries in a cuckoo path from "Algorithmic Improvements for Fast
  // Concurrent Cuckoo Hashing" by Li et al.
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
  // cuckoo filter's bucket number should be a power of 2
  private final int mNumBuckets;
  private final int mBitsPerTag;
  private final int mBitsPerClock;
  private final int mBitsPerSize;
  private final int mBitsPerScope;
  private final int mMaxSize;
  private final int mMaxAge;
  private final Funnel<? super T> mFunnel;
  private final HashFunction mHashFunction;
  private final ScopeEncoder mScopeEncoder;
  private final SegmentedLock mLocks;
  private final int[] mSegmentedAgingPointers;
  // if count-based sliding window, windowSize is the number of operations;
  // if time-based sliding window, windowSize is the milliseconds in a period.
  private final SlidingWindowType mSlidingWindowType;
  private final long mWindowSize;
  private final long mStartTime = System.currentTimeMillis();
  private final CuckooTable mTable;
  private final CuckooTable mClockTable;
  private final CuckooTable mSizeTable;
  private final CuckooTable mScopeTable;

  /**
   * The constructor of concurrent clock cuckoo filter.
   *
   * @param table the table to store tag (fingerprint)
   * @param clockTable the table to store clock
   * @param sizeTable the table to store size
   * @param scopeTable the table to store scope
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of sliding window
   * @param funnel the funnel of T's that the constructed cuckoo filter will use
   * @param hasher the hash function the constructed cuckoo filter will use
   */
  private ConcurrentClockCuckooFilter(CuckooTable table, CuckooTable clockTable,
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
    mMaxSize = (1 << mBitsPerSize);
    mMaxAge = (1 << mBitsPerClock) - 1;
    mSlidingWindowType = slidingWindowType;
    mWindowSize = windowSize;
    mFunnel = funnel;
    mHashFunction = hasher;
    mLocks = new SegmentedLock(Math.min(DEFAULT_NUM_LOCKS, mNumBuckets >> 1), mNumBuckets);
    // init scope statistics
    // note that the GLOBAL scope is the default scope and is always encoded to zero
    mScopeEncoder = new ScopeEncoder(mBitsPerScope);
    mScopeEncoder.encode(CacheScope.GLOBAL);
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
   *      ConcurrentClockCuckooFilter}; must be positive; must be a power of 2
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
    int bitsPerTag = CuckooUtils.optimalBitsPerTag(fpp, loadFactor);
    long numBuckets = CuckooUtils.optimalBuckets(expectedInsertions, loadFactor, TAGS_PER_BUCKET);
    long numBits = numBuckets * TAGS_PER_BUCKET * bitsPerTag;
    BitSet bits = new BuiltinBitSet((int) numBits);
    CuckooTable table = new SimpleCuckooTable(bits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerTag);

    BitSet clockBits = new BuiltinBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerClock));
    CuckooTable clockTable =
        new SimpleCuckooTable(clockBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerClock);

    BitSet sizeBits = new BuiltinBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerSize));
    CuckooTable sizeTable =
        new SimpleCuckooTable(sizeBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerSize);

    BitSet scopeBits = new BuiltinBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerScope));
    CuckooTable scopeTable =
        new SimpleCuckooTable(scopeBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerScope);
    return new ConcurrentClockCuckooFilter<>(table, clockTable, sizeTable, scopeTable,
        slidingWindowType, windowSize, funnel, hasher);
  }

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed {@code BloomFilter} will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      ConcurrentClockCuckooFilter}; must be positive; must be a power of 2
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
   *      ConcurrentClockCuckooFilter}; must be positive; must be a power of 2
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
   *      ConcurrentClockCuckooFilter}; must be positive; must be a power of 2
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
   *      ConcurrentClockCuckooFilter}; must be positive; must be a power of 2
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
  public static <T> ConcurrentClockCuckooFilter<T> create(Funnel<? super T> funnel,
      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope) {
    Preconditions.checkNotNull(funnel);
    Preconditions.checkArgument(expectedInsertions > 0);
    Preconditions.checkArgument(bitsPerClock > 0);
    Preconditions.checkArgument(bitsPerSize > 0);
    Preconditions.checkArgument(bitsPerScope > 0);
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        SlidingWindowType.NONE, -1, DEFAULT_FPP);
  }

  @Override
  public boolean put(T item, int size, CacheScope scopeInfo) {
    // NOTE: zero size is not allowed in our clock filter, because we use zero size as
    // a special case to indicate an size overflow (size > mMaxSize), and all the
    // overflowed size will be revised to mMaxSize in method encodeSize()
    if (size <= 0) {
      return false;
    }
    long hv = hashValue(item);
    int tag = tagHash(hv);
    int b1 = indexHash(hv);
    int b2 = altIndex(b1, tag);
    size = encodeSize(size);
    // Generally, we will hold write locks in two places:
    // 1) put/delete;
    // 2) cuckooPathSearch & cuckooPathMove.
    // But We only execute opportunistic aging in put/delete.
    // This is because we expect cuckoo path search & move to be as fast as possible,
    // or it may be more possible to fail.
    writeLockAndOpportunisticAging(b1, b2);
    TagPosition pos = cuckooInsertLoop(b1, b2, tag);
    if (pos.getStatus() == CuckooStatus.OK) {
      // b1 and b2 should be insertable for item, which means:
      // 1. b1 or b2 have at least one empty slot (this is guaranteed until we unlock two buckets);
      // 2. b1 and b2 do not contain duplicated tag.
      int scope = encodeScope(scopeInfo);
      mTable.writeTag(pos.getBucketIndex(), pos.getSlotIndex(), tag);
      mClockTable.writeTag(pos.getBucketIndex(), pos.getSlotIndex(), mMaxAge);
      mScopeTable.writeTag(pos.getBucketIndex(), pos.getSlotIndex(), scope);
      mSizeTable.writeTag(pos.getBucketIndex(), pos.getSlotIndex(), size);
      // update statistics
      mNumItems.incrementAndGet();
      mTotalBytes.addAndGet(size);
      updateScopeStatistics(scope, 1, size);
      mLocks.unlockWrite(b1, b2);
      return true;
    }
    mLocks.unlockWrite(b1, b2);
    return false;
  }

  @Override
  public boolean mightContainAndResetClock(T item) {
    return mightContainAndOptionalResetClock(item, true);
  }

  @Override
  public boolean mightContain(T item) {
    return mightContainAndOptionalResetClock(item, false);
  }

  /**
   * @param item the item to be checked
   * @param shouldReset the flag to indicate whether to reset clock field
   * @return true if item is in cuckoo filter; false otherwise
   */
  private boolean mightContainAndOptionalResetClock(T item, boolean shouldReset) {
    long hv = hashValue(item);
    int tag = tagHash(hv);
    int b1 = indexHash(hv);
    int b2 = altIndex(b1, tag);
    mLocks.readLock(b1, b2);
    TagPosition pos = mTable.findTag(b1, b2, tag);
    boolean found = pos.getStatus() == CuckooStatus.OK;
    if (found && shouldReset) {
      // set C to MAX
      mClockTable.writeTag(pos.getBucketIndex(), pos.getSlotIndex(), mMaxAge);
    }
    mLocks.unlockRead(b1, b2);
    return found;
  }

  @Override
  public boolean delete(T item) {
    long hv = hashValue(item);
    int tag = tagHash(hv);
    int b1 = indexHash(hv);
    int b2 = altIndex(b1, tag);
    writeLockAndOpportunisticAging(b1, b2);
    TagPosition pos = mTable.deleteTag(b1, tag);
    if (pos.getStatus() != CuckooStatus.OK) {
      pos = mTable.deleteTag(b2, tag);
    }
    if (pos.getStatus() == CuckooStatus.OK) {
      mNumItems.decrementAndGet();
      int scope = mScopeTable.readTag(pos.getBucketIndex(), pos.getSlotIndex());
      int size = mSizeTable.readTag(pos.getBucketIndex(), pos.getSlotIndex());
      size = decodeSize(size);
      updateScopeStatistics(scope, -1, -size);
      // Clear Clock
      mClockTable.writeTag(pos.getBucketIndex(), pos.getSlotIndex(), NON_EXISTENT_TAG);
      mLocks.unlockWrite(b1, b2);
      return true;
    }
    mLocks.unlockWrite(b1, b2);
    return false;
  }

  @Override
  public void aging() {
    int numSegments = mLocks.getNumLocks();
    int bucketsPerSegment = mLocks.getNumBucketsPerSegment();
    for (int i = 0; i < numSegments; i++) {
      // TODO(iluoeli): avoid acquire locks here since it may be blocked
      //  for a long time if this segment is contended by multiple users.
      mLocks.writeLockSegment(i);
      if (mSegmentedAgingPointers[i] < bucketsPerSegment) {
        agingSegment(i, bucketsPerSegment);
      }
      mSegmentedAgingPointers[i] = 0;
      mLocks.unlockWriteSegment(i);
    }
  }

  /**
   * Get the item's clock value (age).
   *
   * @param item the item to be queried
   * @return the clock value
   */
  public int getAge(T item) {
    long hv = hashValue(item);
    int tag = tagHash(hv);
    int b1 = indexHash(hv);
    int b2 = altIndex(b1, tag);
    mLocks.readLock(b1, b2);
    TagPosition pos = mTable.findTag(b1, b2, tag);
    if (pos.getStatus() == CuckooStatus.OK) {
      int clock = mClockTable.readTag(pos.getBucketIndex(), pos.getSlotIndex());
      mLocks.unlockRead(b1, b2);
      return clock;
    }
    mLocks.unlockRead(b1, b2);
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

  @Override
  public double expectedFpp() {
    // equation from "Cuckoo Filter: Simplification and Analysis" by David Eppstein (Theorem 5.1)
    return 2 * mNumItems.doubleValue()
        / (mNumBuckets * TAGS_PER_BUCKET * ((1L << mBitsPerTag) - 1));
  }

  @Override
  public long approximateElementCount() {
    return mNumItems.intValue();
  }

  @Override
  public long approximateElementCount(CacheScope scopeInfo) {
    return mScopeToNumber[encodeScope(scopeInfo)].get();
  }

  @Override
  public long approximateElementSize() {
    return mTotalBytes.get();
  }

  @Override
  public long approximateElementSize(CacheScope scopeInfo) {
    return mScopeToSize[encodeScope(scopeInfo)].get();
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
   * @param item the object to be hashed
   * @return the hash code of this item
   */
  private long hashValue(T item) {
    return mHashFunction.newHasher().putObject(item, mFunnel).hash().asLong();
  }

  /**
   * Compute the index from a hash value.
   *
   * @param hv the hash value used for computing
   * @return the bucket computed on given hash value
   */
  private int indexHash(long hv) {
    return CuckooUtils.indexHash((int) (hv >> 32), mNumBuckets);
  }

  /**
   * Compute the tag from a hash value.
   *
   * @param hv the hash value used for computing
   * @return the tag (fingerprint) computed on given hash value
   */
  private int tagHash(long hv) {
    return CuckooUtils.tagHash((int) hv, mBitsPerTag);
  }

  /**
   * Compute the alternative index from index and tag.
   *
   * @param index the bucket for computing
   * @param tag the tag (fingerprint) for computing
   * @return the alternative bucket
   */
  private int altIndex(int index, int tag) {
    return CuckooUtils.altIndex(index, tag, mNumBuckets);
  }

  /**
   * Encode a scope information into a integer type (storage type). We only store table-level or
   * higher level scope information, so file-level (partition-level) scope will not be stored.
   *
   * @param scopeInfo the scope to be encoded
   * @return the encoded number of scope
   */
  private int encodeScope(CacheScope scopeInfo) {
    if (scopeInfo.level() == CacheScope.Level.PARTITION) {
      return mScopeEncoder.encode(scopeInfo.parent());
    }
    return mScopeEncoder.encode(scopeInfo);
  }

  /**
   * Encode the original size to internal storage types, used for overflow handling etc.
   *
   * @param size the size to be encoded
   * @return the storage type of the encoded size
   */
  private int encodeSize(int size) {
    return Math.min(mMaxSize, size);
  }

  /**
   * Decode the storage type of size to original integer.
   *
   * @param size the storage type of size to be decoded
   * @return the decoded size of the storage type
   */
  private int decodeSize(int size) {
    if (size == 0) {
      size = mMaxSize;
    }
    return size;
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
   * @param tag the tag (fingerprint) of this tem
   * @return a valid tag position pointing to the empty slot; otherwise an invalid tat position
   *         indicating failure
   */
  private TagPosition cuckooInsertLoop(int b1, int b2, int tag) {
    TagPosition pos = cuckooInsert(b1, b2, tag);
    if (pos.getStatus() == CuckooStatus.OK) {
      return pos;
    }
    return new TagPosition(-1, -1, CuckooStatus.FAILURE);
  }

  /**
   * Try find an empty slot for the item. Assume already held the lock of buckets i1 and i2.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param tag the tag (fingerprint)
   * @return a valid tag position pointing to the empty slot; otherwise an invalid tat position
   *         indicating failure
   */
  private TagPosition cuckooInsert(int b1, int b2, int tag) {
    // try find b1 and b2 firstly
    TagPosition pos1 = tryFindInsertBucket(b1, tag);
    if (pos1.getStatus() == CuckooStatus.FAILURE_KEY_DUPLICATED) {
      return pos1;
    }
    TagPosition pos2 = tryFindInsertBucket(b2, tag);
    if (pos2.getStatus() == CuckooStatus.FAILURE_KEY_DUPLICATED) {
      return pos2;
    }
    if (pos1.getStatus() == CuckooStatus.OK) {
      return pos1;
    }
    if (pos2.getStatus() == CuckooStatus.OK) {
      return pos2;
    }
    // then BFS search from b1 and b2
    TagPosition pos = runCuckoo(b1, b2);
    if (pos.getStatus() == CuckooStatus.OK) {
      // avoid another duplicated key is inserted during runCuckoo.
      if (mTable.findTag(b1, b2, tag).getStatus() == CuckooStatus.OK) {
        pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
      }
    }
    return pos;
  }

  /**
   * Try find an empty slot for the item. Assume already held the lock of buckets i1 and i2.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @return a valid tag position pointing to the empty slot; otherwise an invalid tat position
   *         indicating failure
   */
  private TagPosition runCuckoo(int b1, int b2) {
    mLocks.unlockWrite(b1, b2);
    TagPosition pos = new TagPosition(-1, -1, CuckooStatus.FAILURE);
    int maxPathLen = MAX_BFS_PATH_LEN;
    CuckooRecord[] cuckooPath = new CuckooRecord[maxPathLen];
    for (int i = 0; i < maxPathLen; i++) {
      cuckooPath[i] = new CuckooRecord();
    }
    boolean done = false;
    while (!done) {
      int depth = cuckooPathSearch(b1, b2, cuckooPath);
      if (depth < 0) {
        break;
      }
      if (cuckooPathMove(b1, b2, cuckooPath, depth)) {
        pos.setBucketAndSlot(cuckooPath[0].mBucketIndex, cuckooPath[0].mSlotIndex);
        pos.setStatus(CuckooStatus.OK);
        done = true;
      }
    }
    if (!done) {
      // NOTE: since we assume holding the locks of two buckets before calling this method,
      // we keep this assumptions after return.
      mLocks.writeLock(b1, b2);
    }
    return pos;
  }

  /**
   * Search for an empty slot from two initial buckets b1 and b2, and move items backwards along the
   * path.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param cuckooPath the information of searched path
   * @return the depth the path
   */
  private int cuckooPathSearch(int b1, int b2, CuckooRecord[] cuckooPath) {
    // 1. search a path
    BFSEntry x = slotBFSSearch(b1, b2);
    if (x.mDepth == -1) {
      return -1;
    }
    // 2. re-construct path from x
    for (int i = x.mDepth; i >= 0; i--) {
      cuckooPath[i].mSlotIndex = x.mPathcode % TAGS_PER_BUCKET;
      x.mPathcode /= TAGS_PER_BUCKET;
    }
    if (x.mPathcode == 0) {
      cuckooPath[0].mBucketIndex = b1;
    } else {
      cuckooPath[0].mBucketIndex = b2;
    }
    // 3. restore the tag (fingerprint) of the starting point
    mLocks.writeLock(cuckooPath[0].mBucketIndex);
    cuckooPath[0].mTag = mTable.readTag(cuckooPath[0].mBucketIndex, cuckooPath[0].mSlotIndex);
    if (cuckooPath[0].mTag == NON_EXISTENT_TAG) {
      mLocks.unlockWrite(cuckooPath[0].mBucketIndex);
      return 0;
    }
    mLocks.unlockWrite(cuckooPath[0].mBucketIndex);
    // 4. restore the tag (fingerprint) of other points along the cuckoo path
    for (int i = 1; i <= x.mDepth; i++) {
      CuckooRecord curr = cuckooPath[i];
      CuckooRecord prev = cuckooPath[i - 1];
      curr.mBucketIndex = altIndex(prev.mBucketIndex, prev.mTag);
      mLocks.writeLock(curr.mBucketIndex);
      curr.mTag = mTable.readTag(curr.mBucketIndex, curr.mSlotIndex);
      if (curr.mTag == NON_EXISTENT_TAG) {
        mLocks.unlockWrite(curr.mBucketIndex);
        return i;
      }
      mLocks.unlockWrite(curr.mBucketIndex);
    }
    return x.mDepth;
  }

  /**
   * Search an empty slot from two initial buckets.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @return the last entry of searched path
   */
  private BFSEntry slotBFSSearch(int b1, int b2) {
    Queue<BFSEntry> queue = new LinkedList<>();
    queue.offer(new BFSEntry(b1, 0, 0));
    queue.offer(new BFSEntry(b2, 1, 0));
    while (!queue.isEmpty()) {
      BFSEntry x = queue.poll();
      mLocks.writeLock(x.mBucketIndex);
      // pick a random slot to start on
      int startingSlot = x.mPathcode % TAGS_PER_BUCKET;
      for (int i = 0; i < TAGS_PER_BUCKET; i++) {
        int slot = (startingSlot + i) % TAGS_PER_BUCKET;
        int tag = mTable.readTag(x.mBucketIndex, slot);
        if (tag == NON_EXISTENT_TAG) {
          x.mPathcode = x.mPathcode * TAGS_PER_BUCKET + slot;
          mLocks.unlockWrite(x.mBucketIndex);
          return x;
        }
        if (x.mDepth < MAX_BFS_PATH_LEN - 1) {
          queue.offer(new BFSEntry(altIndex(x.mBucketIndex, tag),
              x.mPathcode * TAGS_PER_BUCKET + slot, x.mDepth + 1));
        }
      }
      mLocks.unlockWrite(x.mBucketIndex);
    }
    return new BFSEntry(0, 0, -1);
  }

  /**
   * Move items backward along the cuckoo path.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param cuckooPath the path to move along
   * @param depth the depth of the path
   * @return true if successfully moved items along the path; false otherwise
   */
  private boolean cuckooPathMove(int b1, int b2, CuckooRecord[] cuckooPath, int depth) {
    if (depth == 0) {
      mLocks.writeLock(b1, b2);
      if (mTable.readTag(cuckooPath[0].mBucketIndex, cuckooPath[0].mSlotIndex) == 0) {
        mLocks.unlockWrite(b1, b2);
        return true;
      } else {
        mLocks.unlockWrite(b1, b2);
        return false;
      }
    }

    while (depth > 0) {
      CuckooRecord from = cuckooPath[depth - 1];
      CuckooRecord to = cuckooPath[depth];
      if (depth == 1) {
        // NOTE: We must hold the locks of b1 and b2.
        // Or their slots may be preempted by another key if we released locks.
        mLocks.writeLock(b1, b2, to.mBucketIndex);
      } else {
        mLocks.writeLock(from.mBucketIndex, to.mBucketIndex);
      }
      int fromTag = mTable.readTag(from.mBucketIndex, from.mSlotIndex);
      // if `to` is nonempty, or `from` is not occupied by original tag,
      // in both cases, abort this insertion.
      if (mTable.readTag(to.mBucketIndex, to.mSlotIndex) != NON_EXISTENT_TAG
          || fromTag != from.mTag) {
        if (depth == 1) {
          // NOTE: We must hold the locks of b1 and b2.
          // Or their slots may be preempted by another key if we released locks.
          mLocks.unlockWrite(b1, b2, to.mBucketIndex);
        } else {
          mLocks.unlockWrite(from.mBucketIndex, to.mBucketIndex);
        }
        return false;
      }
      mTable.writeTag(to.mBucketIndex, to.mSlotIndex, fromTag);
      mClockTable.writeTag(to.mBucketIndex, to.mSlotIndex,
          mClockTable.readTag(from.mBucketIndex, from.mSlotIndex));
      mScopeTable.writeTag(to.mBucketIndex, to.mSlotIndex,
          mScopeTable.readTag(from.mBucketIndex, from.mSlotIndex));
      mSizeTable.writeTag(to.mBucketIndex, to.mSlotIndex,
          mSizeTable.readTag(from.mBucketIndex, from.mSlotIndex));
      mTable.writeTag(from.mBucketIndex, from.mSlotIndex, 0);
      if (depth == 1) {
        // if to.mBucketIndex share the same lock with one of b1 and b2, we should not release its
        // lock
        int seg1 = mLocks.getSegmentIndex(b1);
        int seg2 = mLocks.getSegmentIndex(b2);
        int seg3 = mLocks.getSegmentIndex(to.mBucketIndex);
        if (seg3 != seg1 && seg3 != seg2) {
          mLocks.unlockWrite(to.mBucketIndex);
        }
      } else {
        mLocks.unlockWrite(from.mBucketIndex, to.mBucketIndex);
      }
      depth--;
    }
    return true;
  }

  /**
   * Find the `tag` in bucket `i`.
   *
   * @param i the bucket index
   * @param tag the tag (fingerprint)
   * @return true if no duplicated key is found, and `pos.slot` points to an empty slot (if pos.tag
   *         != -1); otherwise return false, and store the position of duplicated key in `pos.slot`.
   */
  private TagPosition tryFindInsertBucket(int i, int tag) {
    TagPosition pos = new TagPosition(i, -1, CuckooStatus.FAILURE_TABLE_FULL);
    for (int slotIndex = 0; slotIndex < TAGS_PER_BUCKET; slotIndex++) {
      int t = mTable.readTag(i, slotIndex);
      if (t != NON_EXISTENT_TAG) {
        if (t == tag) {
          pos.setSlotIndex(slotIndex);
          pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
          return pos;
        }
      } else {
        pos.setSlotIndex(slotIndex);
        pos.setStatus(CuckooStatus.OK);
      }
    }
    return pos;
  }

  /**
   * Lock two buckets and try opportunistic aging. Since we hold write locks, we can assure that
   * there are no other threads aging the same segment.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   */
  private void writeLockAndOpportunisticAging(int b1, int b2) {
    mLocks.writeLock(b1, b2);
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
      bucketsToAge = Math.min(mNumBuckets,
          (int) (mNumBuckets * (elapsedTime / (double) (mWindowSize >> mBitsPerClock))
              - mAgingCount.get()));
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
  private int agingSegment(int i, int maxAgingNumber) {
    int bucketsPerSegment = mLocks.getNumBucketsPerSegment();
    int startPos = mLocks.getSegmentStartPos(i);
    int numAgedBuckets = 0;
    int numCleaned = 0;
    while (numAgedBuckets < maxAgingNumber) {
      int remainingBuckets = bucketsPerSegment - mSegmentedAgingPointers[i];
      if (remainingBuckets == 0) {
        break;
      }
      // age `AGING_STEP_SIZE` buckets and re-check window border
      int bucketsToAge = Math.min(AGING_STEP_SIZE, remainingBuckets);
      // advance agingCount to inform other threads before real aging
      int from = startPos + mSegmentedAgingPointers[i];
      mAgingCount.addAndGet(bucketsToAge);
      mSegmentedAgingPointers[i] += bucketsToAge;
      numCleaned += agingRange(from, from + bucketsToAge);
      numAgedBuckets += bucketsToAge;
    }
    return numCleaned;
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
    for (int slotIndex = 0; slotIndex < TAGS_PER_BUCKET; slotIndex++) {
      int tag = mTable.readTag(b, slotIndex);
      if (tag == NON_EXISTENT_TAG) {
        continue;
      }
      int oldClock = mClockTable.readTag(b, slotIndex);
      if (oldClock > 0) {
        mClockTable.writeTag(b, slotIndex, oldClock - 1);
      } else {
        // evict stale item
        numCleaned++;
        mTable.writeTag(b, slotIndex, NON_EXISTENT_TAG);
        mNumItems.decrementAndGet();
        int scope = mScopeTable.readTag(b, slotIndex);
        int size = mSizeTable.readTag(b, slotIndex);
        size = decodeSize(size);
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
    public int mBucketIndex;
    public int mPathcode; // encode slot position of ancestors and it own nodes
    public int mDepth;

    /**
     * @param bucketIndex the bucket of entry
     * @param pathcode the encoded slot position of ancestors and it own
     * @param depth the depth of the entry in path
     */
    BFSEntry(int bucketIndex, int pathcode, int depth) {
      mBucketIndex = bucketIndex;
      mPathcode = pathcode;
      mDepth = depth;
    }
  }

  /**
   * This class represents a detailed cuckoo record, include its position and stored value.
   */
  static final class CuckooRecord {
    public int mBucketIndex;
    public int mSlotIndex;
    public int mTag;

    CuckooRecord() {
      this(-1, -1, 0);
    }

    /**
     * @param bucketIndex the bucket of this record
     * @param slotIndex the slot of this record
     * @param tag the tag (fingerprint) of this record
     */
    CuckooRecord(int bucketIndex, int slotIndex, int tag) {
      mBucketIndex = bucketIndex;
      mSlotIndex = slotIndex;
      mTag = tag;
    }
  }
}
