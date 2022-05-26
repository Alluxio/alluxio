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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.util.SleepUtils;
import alluxio.wire.RegisterLease;

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

public class RegisterLeaseManagerTest {
  public static final long WORKER1_ID = 1L;
  public static final long WORKER2_ID = 2L;
  public static final long WORKER3_ID = 3L;
  public static final long WORKER4_ID = 4L;

  private RegisterLeaseManager mLeaseManager;

  @Before
  public void before() {
    ServerConfiguration.set(PropertyKey.MASTER_WORKER_REGISTER_LEASE_COUNT, 2);
    ServerConfiguration.set(PropertyKey.MASTER_WORKER_REGISTER_LEASE_TTL, "3s");
    // Tests on the JVM check logic will be done separately
    ServerConfiguration.set(PropertyKey.MASTER_WORKER_REGISTER_LEASE_RESPECT_JVM_SPACE, false);

    mLeaseManager = new RegisterLeaseManager();
  }

  @Test
  public void acquireVerifyRelease() {
    GetRegisterLeasePRequest request = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(WORKER1_ID).setBlockCount(0).build();

    Optional<RegisterLease> lease = mLeaseManager.tryAcquireLease(request);
    assertTrue(lease.isPresent());
    assertTrue(mLeaseManager.hasLease(WORKER1_ID));

    mLeaseManager.releaseLease(WORKER1_ID);
    assertFalse(mLeaseManager.hasLease(WORKER1_ID));
  }

  @Test
  public void recycleExpiredLease() {
    // allocate two leases
    GetRegisterLeasePRequest request1 = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(WORKER1_ID).setBlockCount(0).build();
    Optional<RegisterLease> lease1 = mLeaseManager.tryAcquireLease(request1);
    GetRegisterLeasePRequest request2 = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(WORKER2_ID).setBlockCount(0).build();
    Optional<RegisterLease> lease2 = mLeaseManager.tryAcquireLease(request2);
    assertTrue(lease1.isPresent());
    assertTrue(lease2.isPresent());
    assertTrue(mLeaseManager.hasLease(WORKER1_ID));
    assertTrue(mLeaseManager.hasLease(WORKER2_ID));

    // wait for expiration
    SleepUtils.sleepMs(5000);

    // allocate two more leases
    GetRegisterLeasePRequest request3 = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(WORKER3_ID).setBlockCount(500_000).build();
    Optional<RegisterLease> lease3 = mLeaseManager.tryAcquireLease(request3);
    GetRegisterLeasePRequest request4 = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(WORKER4_ID).setBlockCount(100_000).build();
    Optional<RegisterLease> lease4 = mLeaseManager.tryAcquireLease(request4);

    // The 2 old leases should be recycled
    assertFalse(mLeaseManager.hasLease(WORKER1_ID));
    assertFalse(mLeaseManager.hasLease(WORKER2_ID));
    assertTrue(lease3.isPresent());
    assertTrue(lease4.isPresent());
    assertTrue(mLeaseManager.hasLease(WORKER3_ID));
    assertTrue(mLeaseManager.hasLease(WORKER4_ID));

    // Acquiring one more lease before existing ones expire will be blocked
    GetRegisterLeasePRequest shouldWait = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(5L).setBlockCount(100_000).build();
    Optional<RegisterLease> empty = mLeaseManager.tryAcquireLease(shouldWait);
    assertFalse(empty.isPresent());
    assertFalse(mLeaseManager.hasLease(5L));
  }

  @Test
  public void findExistingLease() {
    GetRegisterLeasePRequest request1 = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(WORKER1_ID).setBlockCount(0).build();
    Optional<RegisterLease> lease1 = mLeaseManager.tryAcquireLease(request1);
    assertTrue(lease1.isPresent());
    assertTrue(mLeaseManager.hasLease(WORKER1_ID));

    // Same worker requesting for an existing lease
    Optional<RegisterLease> sameLease = mLeaseManager.tryAcquireLease(request1);
    assertTrue(sameLease.isPresent());
    assertTrue(mLeaseManager.hasLease(WORKER1_ID));

    // The same lease is granted so there should be another vacancy
    GetRegisterLeasePRequest request2 = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(WORKER2_ID).setBlockCount(0).build();
    Optional<RegisterLease> lease2 = mLeaseManager.tryAcquireLease(request2);
    assertTrue(lease2.isPresent());
    assertTrue(mLeaseManager.hasLease(WORKER2_ID));
    // The lease for worker 1 is not recycled
    assertTrue(mLeaseManager.hasLease(WORKER1_ID));

    // Now all token have been given out, new ones will have to wait
    GetRegisterLeasePRequest shouldWait = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(5L).setBlockCount(100_000).build();
    Optional<RegisterLease> empty = mLeaseManager.tryAcquireLease(shouldWait);
    assertFalse(empty.isPresent());
    assertFalse(mLeaseManager.hasLease(5L));
  }
}
