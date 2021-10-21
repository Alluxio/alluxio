package alluxio.master.block;

import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.wire.RegisterLease;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RegisterLeaseManagerTest {
  // TODO(jiacheng): allocate, release test
  @Test
  public void acquireVerifyRelease() throws Exception {
    RegisterLeaseManager manager = new RegisterLeaseManager();

    GetRegisterLeasePRequest request = GetRegisterLeasePRequest.newBuilder().setWorkerId(1L).setBlockCount(0).build();

    Optional<RegisterLease> lease = manager.tryAcquireLease(request);
    assertTrue(manager.checkLease(1L));

    manager.releaseLease(1L);
    assertFalse(manager.checkLease(1L));
  }
}
