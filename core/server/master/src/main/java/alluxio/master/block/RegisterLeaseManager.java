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
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.metrics.MetricsSystem;
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
      ServerConfiguration.getMs(PropertyKey.MASTER_WORKER_REGISTER_LEASE_TTL);

  private final Semaphore mSemaphore;
  // <WorkerId, ExpiryTime>
  private final Map<Long, RegisterLease> mOpenStreams;

  private JvmSpaceReviewer mJvmChecker;

  /**
   * Constructor.
   */
  public RegisterLeaseManager() {
    int maxConcurrency =
        ServerConfiguration.global().getInt(PropertyKey.MASTER_REGISTER_MAX_CONCURRENCY);
    Preconditions.checkState(maxConcurrency > 0, "%s should be greater than 0",
        PropertyKey.MASTER_REGISTER_MAX_CONCURRENCY.toString());
    mSemaphore = new Semaphore(maxConcurrency);

    if (ServerConfiguration.getBoolean(PropertyKey.MASTER_REGISTER_CHECK_JVM_SPACE)) {
      mJvmChecker = new JvmSpaceReviewer(MetricsSystem.METRIC_REGISTRY);
    }

    mOpenStreams = new ConcurrentHashMap<>();
  }

  // If the lease exists, it will be returned
  Optional<RegisterLease> tryAcquireLease(GetRegisterLeasePRequest request) {
    long workerId = request.getWorkerId();
    if (mOpenStreams.containsKey(workerId)) {
      LOG.info("Found existing lease for worker {}", workerId);
      return Optional.of(mOpenStreams.get(workerId));
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
      mOpenStreams.put(request.getWorkerId(), lease);
      LOG.info("Granted lease to worker, now open streams are {}", mOpenStreams);
      return Optional.of(lease);
    }

    return Optional.empty();
  }

  private void tryRecycleLease() {
    long now = CommonUtils.getCurrentMs();

    mOpenStreams.entrySet().removeIf(entry -> {
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
    return mOpenStreams.containsKey(workerId);
  }

  void releaseLease(long workerId) {
    if (!mOpenStreams.containsKey(workerId)) {
      LOG.info("Worker {}'s lease is not found. Most likely the lease has already been recycled.",
          workerId);
      return;
    }
    mOpenStreams.remove(workerId);
    mSemaphore.release();
  }
}
