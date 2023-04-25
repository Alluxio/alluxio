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

package alluxio.wire;

import alluxio.util.CommonUtils;

import com.google.common.base.MoreObjects;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * A RegisterLease is what the master uses to control the concurrency of worker registration.
 * The worker must be granted a lease before it can start the registration.
 * Each lease will have an expiry timestamp. One lease is released when a worker finishes
 * registration, or the lease expires.
 * The lease is only checked on registration. It is not for worker authentication.
 */
public class RegisterLease {
  public long mExpiryTimeMs;

  /**
   * Constructor.
   *
   * @param ttlMs TTL
   */
  public RegisterLease(long ttlMs) {
    mExpiryTimeMs = CommonUtils.getCurrentMs() + ttlMs;
  }

  @Override
  public String toString() {
    Instant expiry = Instant.ofEpochMilli(mExpiryTimeMs);
    return MoreObjects.toStringHelper(this)
        .add("Expiry",
            DateTimeFormatter.ISO_INSTANT.format(expiry.truncatedTo(ChronoUnit.SECONDS)))
        .toString();
  }
}
