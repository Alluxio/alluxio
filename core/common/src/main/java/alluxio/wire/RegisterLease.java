package alluxio.wire;

import alluxio.util.CommonUtils;
import com.google.common.base.MoreObjects;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Each lease will have an expiry timestamp.
 * The worker must be granted a lease before it is accepted to start the registration.
 * The lease is only checked on registration.
 * One lease is released when a worker finishes registration, or the lease expires.
 */
public class RegisterLease {
  public long mExpiryTimeMs;
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
