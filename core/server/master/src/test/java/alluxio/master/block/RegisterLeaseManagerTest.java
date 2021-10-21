package alluxio.master.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.util.CommonUtils;
import alluxio.util.SleepUtils;
import alluxio.wire.RegisterLease;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RegisterLeaseManagerTest {
  public static final long WORKER1_ID = 1L;
  public static final long WORKER2_ID = 2L;
  public static final long WORKER3_ID = 3L;
  public static final long WORKER4_ID = 4L;

  private RegisterLeaseManager mLeaseManager;

  @Before
  public void before() {
    ServerConfiguration.set(PropertyKey.MASTER_REGISTER_MAX_CONCURRENCY, 2);
    ServerConfiguration.set(PropertyKey.MASTER_WORKER_REGISTER_LEASE_EXPIRY_TIMEOUT, "3s");
    // Tests on the JVM check logic will be done separately
    ServerConfiguration.set(PropertyKey.MASTER_REGISTER_CHECK_JVM_SPACE, false);

    mLeaseManager = new RegisterLeaseManager();
  }

  // TODO(jiacheng): allocate, release test
  @Test
  public void acquireVerifyRelease() {
    GetRegisterLeasePRequest request = GetRegisterLeasePRequest.newBuilder().setWorkerId(WORKER1_ID).setBlockCount(0).build();

    Optional<RegisterLease> lease = mLeaseManager.tryAcquireLease(request);
    assertTrue(lease.isPresent());
    assertTrue(mLeaseManager.checkLease(WORKER1_ID));

    mLeaseManager.releaseLease(WORKER1_ID);
    assertFalse(mLeaseManager.checkLease(WORKER1_ID));
  }

  @Test
  public void recycleExpiredLease() {
    // allocate two leases
    GetRegisterLeasePRequest request1 = GetRegisterLeasePRequest.newBuilder().setWorkerId(WORKER1_ID).setBlockCount(0).build();
    Optional<RegisterLease> lease1 = mLeaseManager.tryAcquireLease(request1);
    GetRegisterLeasePRequest request2 = GetRegisterLeasePRequest.newBuilder().setWorkerId(WORKER2_ID).setBlockCount(0).build();
    Optional<RegisterLease> lease2 = mLeaseManager.tryAcquireLease(request2);
    assertTrue(lease1.isPresent());
    assertTrue(lease2.isPresent());
    assertTrue(mLeaseManager.checkLease(WORKER1_ID));
    assertTrue(mLeaseManager.checkLease(WORKER2_ID));

    // wait for expiration
    System.out.println("Before sleep " + CommonUtils.getCurrentMs());
    SleepUtils.sleepMs(5000);
    System.out.println("After sleep " + CommonUtils.getCurrentMs());

    // allocate two more leases
    GetRegisterLeasePRequest request3 = GetRegisterLeasePRequest.newBuilder().setWorkerId(WORKER3_ID).setBlockCount(500_000).build();
    Optional<RegisterLease> lease3 = mLeaseManager.tryAcquireLease(request3);
    GetRegisterLeasePRequest request4 = GetRegisterLeasePRequest.newBuilder().setWorkerId(WORKER4_ID).setBlockCount(100_000).build();
    Optional<RegisterLease> lease4 = mLeaseManager.tryAcquireLease(request4);

    // The 2 old leases should be recycled
    assertFalse(mLeaseManager.checkLease(WORKER1_ID));
    assertFalse(mLeaseManager.checkLease(WORKER2_ID));
    assertTrue(lease3.isPresent());
    assertTrue(lease4.isPresent());
    assertTrue(mLeaseManager.checkLease(WORKER3_ID));
    assertTrue(mLeaseManager.checkLease(WORKER4_ID));

    // Acquiring one more lease before existing ones expire will be blocked
    GetRegisterLeasePRequest shouldWait = GetRegisterLeasePRequest.newBuilder().setWorkerId(5L).setBlockCount(100_000).build();
    Optional<RegisterLease> empty = mLeaseManager.tryAcquireLease(shouldWait);
    assertFalse(empty.isPresent());
    assertFalse(mLeaseManager.checkLease(5L));
  }

  @Test
  public void findExistingLease() {
    GetRegisterLeasePRequest request1 = GetRegisterLeasePRequest.newBuilder().setWorkerId(WORKER1_ID).setBlockCount(0).build();
    Optional<RegisterLease> lease1 = mLeaseManager.tryAcquireLease(request1);
    assertTrue(lease1.isPresent());
    assertTrue(mLeaseManager.checkLease(WORKER1_ID));

    // Same worker requesting for an existing lease
    Optional<RegisterLease> sameLease = mLeaseManager.tryAcquireLease(request1);
    assertTrue(sameLease.isPresent());
    assertTrue(mLeaseManager.checkLease(WORKER1_ID));

    // The same lease is granted so there should be another vacancy
    GetRegisterLeasePRequest request2 = GetRegisterLeasePRequest.newBuilder().setWorkerId(WORKER2_ID).setBlockCount(0).build();
    Optional<RegisterLease> lease2 = mLeaseManager.tryAcquireLease(request2);
    assertTrue(lease2.isPresent());
    assertTrue(mLeaseManager.checkLease(WORKER2_ID));
    // The lease for worker 1 is not recycled
    assertTrue(mLeaseManager.checkLease(WORKER1_ID));

    // Now all token have been given out, new ones will have to wait
    GetRegisterLeasePRequest shouldWait = GetRegisterLeasePRequest.newBuilder().setWorkerId(5L).setBlockCount(100_000).build();
    Optional<RegisterLease> empty = mLeaseManager.tryAcquireLease(shouldWait);
    assertFalse(empty.isPresent());
    assertFalse(mLeaseManager.checkLease(5L));
  }



  // test in BlockMasterWorkerServiceHandler


}
