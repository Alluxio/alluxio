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

package alluxio.master.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.Configuration;
import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.util.CommonUtils;
import alluxio.wire.RegisterLease;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * The manager for {@link RegisterLease} that manages the lifecycle for the leases.
 */
public class RegisterLeaseManager {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterLeaseManager.class);
  private static final long LEASE_TTL_MS =
      Configuration.getMs(PropertyKey.MASTER_WORKER_REGISTER_LEASE_TTL);

  private final Semaphore mSemaphore;
  private final Map<Long, RegisterLease> mActiveLeases;

  private JvmSpaceReviewer mJvmChecker = null;

  /**
   * Constructor.
   */
  public RegisterLeaseManager() {
    int maxConcurrency =
        Configuration.getInt(PropertyKey.MASTER_WORKER_REGISTER_LEASE_COUNT);
    Preconditions.checkState(maxConcurrency > 0, "%s should be greater than 0",
        PropertyKey.MASTER_WORKER_REGISTER_LEASE_COUNT.toString());
    mSemaphore = new Semaphore(maxConcurrency);

    if (Configuration.getBoolean(PropertyKey.MASTER_WORKER_REGISTER_LEASE_RESPECT_JVM_SPACE)) {
      mJvmChecker = new JvmSpaceReviewer(Runtime.getRuntime());
    }

    mActiveLeases = new ConcurrentHashMap<>();
  }

  // If the lease exists, it will be returned
  Optional<RegisterLease> tryAcquireLease(GetRegisterLeasePRequest request) {
    long workerId = request.getWorkerId();
    if (mActiveLeases.containsKey(workerId)) {
      RegisterLease lease = mActiveLeases.get(workerId);
      LOG.info("Found existing lease for worker {}: {}", workerId, lease);
      return Optional.of(lease);
    }

    // If the JVM space does not allow, reject the request
    if (mJvmChecker != null && !mJvmChecker.reviewLeaseRequest(request)) {
      return Optional.empty();
    }

    // Check for expired leases here instead of having a separate thread.
    // Therefore the recycle is lazy.
    // If no other worker is requesting for a lease, a worker holding an expired lease
    // will still be admitted.
    tryRecycleLease();

    if (mSemaphore.tryAcquire()) {
      RegisterLease lease = new RegisterLease(LEASE_TTL_MS);
      mActiveLeases.put(workerId, lease);
      LOG.info("Granted lease to worker {}", workerId);
      return Optional.of(lease);
    }

    return Optional.empty();
  }

  private void tryRecycleLease() {
    long now = CommonUtils.getCurrentMs();

    mActiveLeases.entrySet().removeIf(entry -> {
      long expiry = entry.getValue().mExpiryTimeMs;
      if (expiry < now) {
        LOG.debug("Lease {} has expired and been recycled.", entry.getKey());
        mSemaphore.release();
        return true;
      }
      return false;
    });
  }

  boolean hasLease(long workerId) {
    return mActiveLeases.containsKey(workerId);
  }

  void releaseLease(long workerId) {
    if (!mActiveLeases.containsKey(workerId)) {
      LOG.info("Worker {}'s lease is not found. Most likely the lease has already been recycled.",
          workerId);
      return;
    }
    mActiveLeases.remove(workerId);
    mSemaphore.release();
  }
}
