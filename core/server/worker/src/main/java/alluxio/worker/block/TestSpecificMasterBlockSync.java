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

package alluxio.worker.block;

import alluxio.exception.FailedToAcquireRegisterLeaseException;
import alluxio.exception.runtime.UnavailableRuntimeException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A test {@link SpecificMasterBlockSync} that adds some interfaces for testing.
 */
@NotThreadSafe
@VisibleForTesting
public final class TestSpecificMasterBlockSync extends SpecificMasterBlockSync {
  private static final Logger LOG = LoggerFactory.getLogger(TestSpecificMasterBlockSync.class);
  private volatile boolean mFailHeartbeat = false;
  private final AtomicInteger mRegistrationSuccessCount = new AtomicInteger(0);

  /**
   * Creates a new instance of {@link SpecificMasterBlockSync}.
   *
   * @param blockWorker the {@link BlockWorker} this syncer is updating to
   * @param masterClient the block master client
   * @param heartbeatReporter the heartbeat reporter
   */
  public TestSpecificMasterBlockSync(
      BlockWorker blockWorker, BlockMasterClient masterClient,
      BlockHeartbeatReporter heartbeatReporter) throws IOException {
    super(blockWorker, masterClient, heartbeatReporter);
  }

  /**
   * Restores the heartbeat.
   */
  public void restoreHeartbeat() {
    mFailHeartbeat = false;
  }

  /**
   * Fails the heartbeat and lets it throws an exception.
   */
  public void failHeartbeat() {
    mFailHeartbeat = true;
  }

  /**
   * @return registration success count
   */
  public int getRegistrationSuccessCount() {
    return mRegistrationSuccessCount.get();
  }

  @Override
  protected void registerWithMasterInternal()
      throws IOException, FailedToAcquireRegisterLeaseException {
    super.registerWithMasterInternal();
    mRegistrationSuccessCount.incrementAndGet();
  }

  @Override
  protected void beforeHeartbeat() {
    if (mFailHeartbeat) {
      throw new UnavailableRuntimeException("Heartbeat paused");
    }
  }
}
