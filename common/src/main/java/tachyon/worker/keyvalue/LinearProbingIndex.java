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
import java.util.Arrays;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import tachyon.util.io.ByteIOUtils;

/**
 * Index structure using linear probing. It keeps a collection of buckets. Each bucket stores a
 * fingerprint (in byte) and a offset (in int) indicating where to find the key and value in
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
  /** Max number of probes for linear probing */
  private static final int MAX_PROBES = 50;
  /** Size of each bucket in bytes */
  private static final int BUCKET_SIZE_BYTES = (Byte.SIZE + Integer.SIZE) / Byte.SIZE;
  /** Hash function to calculate bucket index */
  private final HashFunction mIndexHasher;
  /** Hash function to calculate fingerprint */
  private final HashFunction mFingerprintHasher;

  private byte[] mBuf;
  private int mNumBuckets;
  private int mKeyCount;

  /**
   * @return an instance of linear probing index, with no key added
   */
  public static LinearProbingIndex createEmptyIndex() {
    LinearProbingIndex index = new LinearProbingIndex();
    Arrays.fill(index.toByteArray(), (byte) 0);
    return index;
  }

  public LinearProbingIndex() {
    mIndexHasher = Hashing.murmur3_32(INDEX_HASHER_SEED);
    mFingerprintHasher = Hashing.murmur3_32(FINGERPRINT_HASHER_SEED);
    mNumBuckets = 1 << 15;
    mBuf = new byte[mNumBuckets * BUCKET_SIZE_BYTES];
    mKeyCount = 0;
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
  public boolean put(byte[] key, int offset) throws IOException {
    int index = indexHash(key);
    // Linear probing until next empty bucket is found
    for (int probe = 0; probe < MAX_PROBES; probe ++) {
      int pos = index * BUCKET_SIZE_BYTES;
      byte fingerprint = ByteIOUtils.readByte(mBuf, pos);
      if (fingerprint == 0) {
        // bucket is empty
        ByteIOUtils.writeByte(mBuf, pos ++, fingerprintHash(key));
        ByteIOUtils.writeInt(mBuf, pos, offset);
        mKeyCount ++;
        return true;
      }
      index = (index + 1) % mNumBuckets;
    }
    return false;
  }

  @Override
  public byte[] get(byte[] key, PayloadReader reader) {
    int index = indexHash(key);
    byte fingerprint = fingerprintHash(key);

    // Linear probing until next empty bucket is found
    for (int probe = 0; probe < MAX_PROBES; probe ++) {
      int pos = index * BUCKET_SIZE_BYTES;
      if (fingerprint == ByteIOUtils.readByte(mBuf, pos)) {
        int offset = ByteIOUtils.readInt(mBuf, pos + 1);
        byte[] keyStored = reader.getKey(offset);
        if (Arrays.equals(key, keyStored)) {
          return reader.getValue(offset);
        }
      }
      index = (index + 1) % mNumBuckets;
    }
    return null;
  }

  @Override
  public byte[] toByteArray() {
    return mBuf;
  }

  /**
   * Hashes the key to a bucket index in non-negative integer value.
   *
   * @param key key in byte array
   * @return bucket index of key
   */
  public int indexHash(byte[] key) {
    // TODO(binfan): change mod to bit-and
    int v = mIndexHasher.hashBytes(key).asInt() % mNumBuckets;
    return (v >= 0) ? v : -v;
  }


  /**
   * Hashes the key into a non-zero fingerprint in byte.
   *
   * @param key key in byte array
   * @return value of fingerprint in byte which is never zero
   */
  public byte fingerprintHash(byte[] key) {
    int hash = mFingerprintHasher.hashBytes(key).asInt();
    hash = (hash >> 24) & 0xff; // use high-order bits
    return (byte) ((hash == 0) ? 1 : hash);
  }
}
