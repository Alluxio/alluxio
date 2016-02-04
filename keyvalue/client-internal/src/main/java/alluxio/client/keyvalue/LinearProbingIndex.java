/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client.keyvalue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import alluxio.Constants;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.ByteIOUtils;

/**
 * Index structure using linear probing. It keeps a collection of buckets. Each bucket stores a
 * fingerprint (a byte) and an offset (an int) indicating where to find the key and the value in
 * the payload.
 * <p>
 * The index hash table looks like:
 * =====================================
 * | fingerprint (byte) | offset (int) |
 * =====================================
 * | fingerprint (byte) | offset (int) |
 * =====================================
 * |                ...                |
 *
 * If fingerprint is zero, it indicates the bucket is empty.
 * <p>
 * The overall byte array of the index looks like:
 * | key count (int) | the above hash table |
 */
@NotThreadSafe
public final class LinearProbingIndex implements Index {
  /** Max number of probes for linear probing */
  public static final int MAX_PROBES = 50;

  // TODO(binfan): pick better seeds
  private static final int INDEX_HASHER_SEED = 0x1311;
  private static final int FINGERPRINT_HASHER_SEED = 0x7a91;
  /** Hash function to calculate bucket index */
  private static final HashFunction INDEX_HASHER = Hashing.murmur3_32(INDEX_HASHER_SEED);
  /** Hash function to calculate fingerprint */
  private static final HashFunction FINGERPRINT_HASHER =
      Hashing.murmur3_32(FINGERPRINT_HASHER_SEED);
  /** Size of each bucket in bytes */
  private static final int BUCKET_SIZE_BYTES = Constants.BYTES_IN_INTEGER + 1;

  /**
   * Total byte buffer of the index, including a key count and a hash table. This is to avoid
   * byte array copy when calling {@link #getBytes()}.
   */
  private ByteBuffer mBuf;
  /**
   * Number of keys, when calling {@link #getBytes()}, it is written to the first four bytes of
   * {@link #mBuf}.
   */
  private int mKeyCount;
  /** A slice of mBuf corresponding to the hash table part */
  private ByteBuffer mHashTableBuf;
  private int mNumBuckets;

  /**
   * @return an instance of linear probing index, with no key added
   */
  public static LinearProbingIndex createEmptyIndex() {
    int numBuckets = 1 << 15;
    byte[] buffer = new byte[Constants.BYTES_IN_INTEGER + numBuckets * BUCKET_SIZE_BYTES];
    return new LinearProbingIndex(ByteBuffer.wrap(buffer), numBuckets, 0);
  }

  /**
   * Creates an instance of linear probing index by loading its content from a buffer. The
   * {@link ByteBuffer#position} must be at the beginning of index.
   *
   * @param buffer intput buffer storing the index
   * @return an instance of linear probing index
   */
  public static LinearProbingIndex loadFromByteArray(ByteBuffer buffer) {
    int keyCount = buffer.getInt();
    int numBuckets = buffer.remaining() / BUCKET_SIZE_BYTES;
    return new LinearProbingIndex(buffer, numBuckets, keyCount);
  }

  private LinearProbingIndex(ByteBuffer buf, int numBuckets, int keyCount) {
    mBuf = buf;
    mHashTableBuf = BufferUtils.sliceByteBuffer(mBuf, Constants.BYTES_IN_INTEGER);
    mNumBuckets = numBuckets;
    mKeyCount = keyCount;
  }

  @Override
  public int byteCount() {
    return Constants.BYTES_IN_INTEGER + mNumBuckets * BUCKET_SIZE_BYTES;
  }

  @Override
  public int keyCount() {
    return mKeyCount;
  }

  @Override
  public boolean put(byte[] key, byte[] value, PayloadWriter writer) throws IOException {
    int bucketIndex = indexHash(key);
    int pos = bucketIndex * BUCKET_SIZE_BYTES;
    // Linear probing until the next empty bucket (fingerprint is 0) is found
    for (int probe = 0; probe < MAX_PROBES; probe ++) {
      byte fingerprint = ByteIOUtils.readByte(mHashTableBuf, pos);
      if (fingerprint == 0) {
        // bucket is empty
        // Pack key and value into a byte array payload
        final int offset = writer.insert(key, value);
        ByteIOUtils.writeByte(mHashTableBuf, pos ++, fingerprintHash(key));
        ByteIOUtils.writeInt(mHashTableBuf, pos, offset);
        mKeyCount ++;
        return true;
      }
      bucketIndex = (bucketIndex + 1) % mNumBuckets;
      pos = (bucketIndex == 0) ? 0 : pos + BUCKET_SIZE_BYTES;
    }
    return false;
  }

  @Override
  public ByteBuffer get(ByteBuffer key, PayloadReader reader) {
    int bucketOffset = bucketOffset(key, reader);
    if (bucketOffset == - 1) {
      return null;
    }
    return reader.getValue(ByteIOUtils.readInt(mHashTableBuf, bucketOffset + 1));
  }

  /**
   * @param key the key
   * @param reader the reader to be used to get key from payload
   * @return bucket offset in the {@link Index} of the key, -1 if no such key is found
   */
  private int bucketOffset(ByteBuffer key, PayloadReader reader) {
    int bucketIndex = indexHash(key);
    byte fingerprint = fingerprintHash(key);
    int bucketOffset = bucketIndex * BUCKET_SIZE_BYTES;
    // Linear probing until a bucket having the same key is found.
    for (int probe = 0; probe < MAX_PROBES; probe ++) {
      if (fingerprint == ByteIOUtils.readByte(mHashTableBuf, bucketOffset)) {
        int offset = ByteIOUtils.readInt(mHashTableBuf, bucketOffset + 1);
        ByteBuffer keyStored = reader.getKey(offset);
        if (key.equals(keyStored)) {
          return bucketOffset;
        }
      }
      bucketIndex = (bucketIndex + 1) % mNumBuckets;
      bucketOffset = (bucketIndex == 0) ? 0 : bucketOffset + BUCKET_SIZE_BYTES;
    }
    return -1;
  }

  @Override
  public byte[] getBytes() {
    // No byte array copy happens.
    mBuf.putInt(0, mKeyCount);
    return mBuf.array();
  }

  /**
   * Hashes a key in byte array to a bucket index in non-negative integer value.
   *
   * @param key key in byte array
   * @return bucket index of key
   */
  public int indexHash(byte[] key) {
    // TODO(binfan): change mod to bit-and
    int v = INDEX_HASHER.hashBytes(key).asInt() % mNumBuckets;
    return (v >= 0) ? v : -v;
  }

  /**
   * Hashes a key in {@code ByteBuffer} to a bucket index in non-negative integer value.
   *
   * @param key key in byte array
   * @return bucket index of key
   */
  public int indexHash(ByteBuffer key) {
    byte[] keyBytes = BufferUtils.newByteArrayFromByteBuffer(key);
    return indexHash(keyBytes);
  }

  /**
   * Hashes a key in byte array into a non-zero, one byte fingerprint.
   *
   * @param key key in byte array
   * @return value of fingerprint in byte which is never zero
   */
  public byte fingerprintHash(byte[] key) {
    int hash = FINGERPRINT_HASHER.hashBytes(key).asInt();
    hash = (hash >> 24) & 0xff; // use high-order bits
    return (byte) ((hash == 0) ? 1 : hash);
  }

  /**
   * Hashes a key in {@code ByteBuffer} into a non-zero, one byte fingerprint.
   *
   * @param key key in byte array
   * @return value of fingerprint in byte which is never zero
   */
  public byte fingerprintHash(ByteBuffer key) {
    byte[] keyBytes = BufferUtils.newByteArrayFromByteBuffer(key);
    return fingerprintHash(keyBytes);
  }

  @Override
  public ByteBuffer nextKey(ByteBuffer currentKey, PayloadReader reader) {
    int nextBucketOffset =
        currentKey == null ? 0 : bucketOffset(currentKey, reader) + BUCKET_SIZE_BYTES;
    final int bufLimit = mHashTableBuf.limit();
    while (nextBucketOffset < bufLimit) {
      byte fingerprint = ByteIOUtils.readByte(mHashTableBuf, nextBucketOffset);
      if (fingerprint != 0) {
        return reader.getKey(ByteIOUtils.readInt(mHashTableBuf, nextBucketOffset + 1));
      }
      nextBucketOffset += BUCKET_SIZE_BYTES;
    }
    return null;
  }

  @Override
  public Iterator<ByteBuffer> keyIterator(final PayloadReader reader) {
    return new Iterator<ByteBuffer>() {
      private final int mBufLimit = mHashTableBuf.limit();
      private int mOffset = 0;
      private int mKeyIndex = 0;

      @Override
      public boolean hasNext() {
        return mKeyIndex < mKeyCount;
      }

      @Override
      public ByteBuffer next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        while (mOffset < mBufLimit) {
          byte fingerprint = ByteIOUtils.readByte(mHashTableBuf, mOffset);
          if (fingerprint != 0) {
            int offset = ByteIOUtils.readInt(mHashTableBuf, mOffset + 1);
            ByteBuffer key = reader.getKey(offset);
            mOffset += BUCKET_SIZE_BYTES;
            mKeyIndex ++;
            return key;
          }
          mOffset += BUCKET_SIZE_BYTES;
        }
        throw new NoSuchElementException();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
