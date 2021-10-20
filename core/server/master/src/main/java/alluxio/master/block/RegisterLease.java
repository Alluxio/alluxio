package alluxio.master.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

/**
 * Each lease will have an expiry timestamp.
 * The worker must be granted a lease before it is accepted to start the registration.
 * The lease is only checked on registration.
 * One lease is released when a worker finishes registration, or the lease expires.
 */
public class RegisterLease {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterLease.class);
  private final long mTimeout = ServerConfiguration.getMs(PropertyKey.MASTER_WORKER_REGISTER_LEASE_EXPIRY_TIMEOUT);

  public Instant mExpireTime;
  public RegisterLease() {
    Instant now = Instant.now();
    mExpireTime = now.plus(mTimeout, ChronoUnit.MILLIS);
    LOG.info("Creating a lease with now={}, expiry={} with timeout={}", now.toEpochMilli(), mExpireTime.toEpochMilli(), mTimeout);
  }
}
