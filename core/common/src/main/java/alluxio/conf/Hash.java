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

package alluxio.conf;

import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A hex encoded MD5 hash of the cluster or path configurations.
 */
@ThreadSafe
public final class Hash {
  private final MessageDigest mMD5;
  private final Supplier<Stream<byte[]>> mProperties;
  private final AtomicBoolean mShouldUpdate;
  private volatile String mVersion;

  /**
   * @param properties a stream of encoded properties
   */
  public Hash(Supplier<Stream<byte[]>> properties) {
    try {
      mMD5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    mProperties = properties;
    mShouldUpdate = new AtomicBoolean(true);
  }

  /**
   * Called when the version needs to be updated.
   * Internally, it just sets a flag to indicate that the version should be updated,
   * but version will not be recomputed until {@link #get()} is called.
   */
  public void markOutdated() {
    mShouldUpdate.set(true);
  }

  private String compute() {
    mMD5.reset();
    mProperties.get().forEach(property -> mMD5.update(property));
    return Hex.encodeHexString(mMD5.digest());
  }

  /**
   * If {@link #markOutdated()} is called since last {@link #get()}, then the version will be
   * recomputed, otherwise, the internally cached version is returned.
   *
   * @return the latest version
   */
  public String get() {
    if (mShouldUpdate.get()) {
      synchronized (this) {
        // If another thread has recomputed the version, no need to recompute again.
        if (mShouldUpdate.get()) {
          mVersion = compute();
          mShouldUpdate.set(false);
        }
      }
    }
    return mVersion;
  }
}
