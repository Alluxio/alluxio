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

package tachyon.worker.keyvalue;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import tachyon.Constants;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.ByteIOUtils;

/**
 * Index structure using linear probing. It keeps a collection of buckets. Each bucket stores a
 * fingerprint (in byte) and an offset (in int) indicating where to find the key and value in
 * payload.
 *
 * | fingerprint (byte) | offset (int) |
 *
 * If fingerprint is zero, it indicates the bucket is empty.
 *
 * This class is not thread-safe.
 */
public final class LinearProbingIndex implements Index {
  // TODO(binfan): pick better seeds
  private static final int INDEX_HASHER_SEED = 0x1311;
  private static final int FINGERPRINT_HASHER_SEED = 0x7a91;
  /** Hash function to calculate bucket index */
  private static final HashFunction INDEX_HASHER = Hashing.murmur3_32(INDEX_HASHER_SEED);
  /** Hash function to calculate fingerprint */
  private static final HashFunction FINGERPRINT_HASHER =
      Hashing.murmur3_32(FINGERPRINT_HASHER_SEED);
  /** Max number of probes for linear probing */
  private static final int MAX_PROBES = 50;
  /** Size of each bucket in bytes */
  private static final int BUCKET_SIZE_BYTES = Constants.BYTES_IN_INTEGER + 1;

  private ByteBuffer mBuf;
  private int mNumBuckets;
  private int mKeyCount;

  /**
   * @return an instance of linear probing index, with no key added
   */
  public static LinearProbingIndex createEmptyIndex() {
    int numBuckets = 1 << 15;
    byte[] buffer = new byte[numBuckets * BUCKET_SIZE_BYTES];
    return new LinearProbingIndex(ByteBuffer.wrap(buffer), numBuckets, 0);
  }

  public static LinearProbingIndex loadFromByteArray(ByteBuffer buffer) {
    int numBuckets = buffer.limit() / BUCKET_SIZE_BYTES;
    // TODO(binfan): fix the key count which is wrong now.
    return new LinearProbingIndex(buffer, numBuckets, 0);
  }

  private LinearProbingIndex(ByteBuffer buf, int numBuckets, int keyCount) {
    mBuf = buf;
    mNumBuckets = numBuckets;
    mKeyCount = keyCount;
  }

  @Override
  public int byteCount() {
    return mNumBuckets * BUCKET_SIZE_BYTES;
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
      byte fingerprint = ByteIOUtils.readByte(mBuf, pos);
      if (fingerprint == 0) {
        // bucket is empty
        // Pack key and value into a byte array payload
        final int offset = writer.insert(key, value);
        ByteIOUtils.writeByte(mBuf, pos ++, fingerprintHash(key));
        ByteIOUtils.writeInt(mBuf, pos, offset);
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
    int bucketIndex = indexHash(key);
    byte fingerprint = fingerprintHash(key);
    int bucketOffset = bucketIndex * BUCKET_SIZE_BYTES;
    // Linear probing until a bucket having the same fingerprint is found
    for (int probe = 0; probe < MAX_PROBES; probe ++) {
      if (fingerprint == ByteIOUtils.readByte(mBuf, bucketOffset)) {
        int offset = ByteIOUtils.readInt(mBuf, bucketOffset + 1);
        ByteBuffer keyStored = reader.getKey(offset);
        if (key.equals(keyStored)) {
          return reader.getValue(offset);
        }
      }
      bucketIndex = (bucketIndex + 1) % mNumBuckets;
      bucketOffset = (bucketIndex == 0) ? 0 : bucketOffset + BUCKET_SIZE_BYTES;
    }
    return null;
  }

  @Override
  public byte[] getBytes() {
    return mBuf.array();
  }

  /**
   * Hashes the key to a bucket index in non-negative integer value.
   *
   * @param key key in byte array
   * @return bucket index of key
   */
  public int indexHash(byte[] key) {
    // TODO(binfan): change mod to bit-and
    int v = INDEX_HASHER.hashBytes(key).asInt() % mNumBuckets;
    return (v >= 0) ? v : -v;
  }

  public int indexHash(ByteBuffer key) {
    byte[] keyBytes = BufferUtils.newByteArrayFromByteBuffer(key);
    return indexHash(keyBytes);
  }

  /**
   * Hashes the key into a non-zero fingerprint in byte.
   *
   * @param key key in byte array
   * @return value of fingerprint in byte which is never zero
   */
  public byte fingerprintHash(byte[] key) {
    int hash = FINGERPRINT_HASHER.hashBytes(key).asInt();
    hash = (hash >> 24) & 0xff; // use high-order bits
    return (byte) ((hash == 0) ? 1 : hash);
  }

  public byte fingerprintHash(ByteBuffer key) {
    byte[] keyBytes = BufferUtils.newByteArrayFromByteBuffer(key);
    return fingerprintHash(keyBytes);
  }
}
