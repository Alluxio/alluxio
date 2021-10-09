package alluxio.master.block;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

public class RegisterLease {
  public Instant mExpireTime;
  public RegisterLease() {
    mExpireTime = Instant.now().plus(5, ChronoUnit.MINUTES);
  }
}
