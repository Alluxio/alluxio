/*
 * Written by Doug Lea with assistance from members of JCP JSR-166 Expert Group and released to the
 * public domain, as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */
package alluxio.concurrent.jsr;

import java.security.AccessControlContext;
import java.security.PrivilegedAction;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A random number generator isolated to the current thread.
 *
 * @since 1.7
 * @author Doug Lea
 */
final class TLRandom {
  // CVS rev. 1.58

  /**
   * The increment for generating probe values.
   */
  private static final int PROBE_INCREMENT = 0x9e3779b9;
  /**
   * The increment of seeder per new instance.
   */
  private static final long SEEDER_INCREMENT = 0xbb67ae8584caa73bL;
  // Unsafe mechanics
  private static final sun.misc.Unsafe U = UnsafeAccess.unsafe;
  private static final long THREADLOCALS;

  // Within-package utilities

  /*
   * Descriptions of the usages of the methods below can be found in the classes that use them.
   * Briefly, a thread's "probe" value is a non-zero hash code that (probably) does not collide with
   * other existing threads with respect to any power of two collision space. When it does collide,
   * it is pseudo-randomly adjusted (using a Marsaglia XorShift). The nextSecondarySeed method is
   * used in the same contexts as ThreadLocalRandom, but only for transient usages such as random
   * adaptive spin/block sequences for which a cheap RNG suffices and for which it could in
   * principle disrupt user-visible statistical properties of the main ThreadLocalRandom if we were
   * to use it.
   */
  private static final long INHERITABLETHREADLOCALS;
  private static final long INHERITEDACCESSCONTROLCONTEXT;
  private static final long CCL;
  private static final long PROBE;
  private static final long SECONDARY;
  /** Generates per-thread initialization/probe field */
  private static final AtomicInteger probeGenerator = new AtomicInteger();
  /**
   * The next seed for default constructors.
   */
  private static final AtomicLong seeder =
      new AtomicLong(mix64(System.currentTimeMillis()) ^ mix64(System.nanoTime()));

  static {
    try {
      THREADLOCALS = U.objectFieldOffset(Thread.class.getDeclaredField("threadLocals"));
      INHERITABLETHREADLOCALS =
          U.objectFieldOffset(Thread.class.getDeclaredField("inheritableThreadLocals"));
      INHERITEDACCESSCONTROLCONTEXT =
          U.objectFieldOffset(Thread.class.getDeclaredField("inheritedAccessControlContext"));
      CCL = U.objectFieldOffset(Thread.class.getDeclaredField("contextClassLoader"));
      PROBE = U.objectFieldOffset(Thread.class.getDeclaredField("threadLocalRandomProbe"));
      SECONDARY =
          U.objectFieldOffset(Thread.class.getDeclaredField("threadLocalRandomSecondarySeed"));
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // Support for other package-private ThreadLocal access

  // at end of <clinit> to survive static initialization circularity
  static {
    if (java.security.AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        return Boolean.getBoolean("java.util.secureRandomSeed");
      }
    })) {
      byte[] seedBytes = java.security.SecureRandom.getSeed(8);
      long s = (long) seedBytes[0] & 0xffL;
      for (int i = 1; i < 8; ++i) {
        s = (s << 8) | ((long) seedBytes[i] & 0xffL);
      }
      seeder.set(s);
    }
  }

  private TLRandom() {}

  private static long mix64(long z) {
    z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
    z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
    return z ^ (z >>> 33);
  }

  // Static initialization

  private static int mix32(long z) {
    z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
    return (int) (((z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L) >>> 32);
  }

  /**
   * Initialize Thread fields for the current thread. Called only when Thread.threadLocalRandomProbe
   * is zero, indicating that a thread local seed value needs to be generated. Note that even though
   * the initialization is purely thread-local, we need to rely on (static) atomic generators to
   * initialize the values.
   */
  static final void localInit() {
    int p = probeGenerator.addAndGet(PROBE_INCREMENT);
    int probe = (p == 0) ? 1 : p; // skip 0
    long seed = mix64(seeder.getAndAdd(SEEDER_INCREMENT));
    utilizeSeed(seed);
    setThreadLocalRandomProbe(probe);
  }

  /**
   * Returns the probe value for the current thread without forcing initialization. Note that
   * invoking ThreadLocalRandom.current() can be used to force initialization on zero return.
   */
  static final int getProbe() {
    return getThreadLocalRandomProbe();
  }

  /**
   * Pseudo-randomly advances and records the given probe value for the given thread.
   */
  static final int advanceProbe(int probe) {
    probe ^= probe << 13; // xorshift
    probe ^= probe >>> 17;
    probe ^= probe << 5;
    setThreadLocalRandomProbe(probe);
    return probe;
  }

  /**
   * Returns the pseudo-randomly initialized or updated secondary seed.
   */
  static final int nextSecondarySeed() {
    int r;
    if ((r = getThreadLocalRandomSecondarySeed()) != 0) {
      r ^= r << 13; // xorshift
      r ^= r >>> 17;
      r ^= r << 5;
    } else if ((r = mix32(seeder.getAndAdd(SEEDER_INCREMENT))) == 0) {
      r = 1; // avoid zero
    }
    setThreadLocalRandomSecondarySeed(r);
    return r;
  }

  private static void utilizeSeed(long seed) {
    Objects.requireNonNull(Long.valueOf(seed));
  }

  private static int getThreadLocalRandomProbe() {
    return U.getInt(Thread.currentThread(), PROBE);
  }

  private static void setThreadLocalRandomProbe(int probe) {
    U.putInt(Thread.currentThread(), PROBE, probe);
  }

  private static int getThreadLocalRandomSecondarySeed() {
    return U.getInt(Thread.currentThread(), SECONDARY);
  }

  private static void setThreadLocalRandomSecondarySeed(int secondary) {
    U.putInt(Thread.currentThread(), SECONDARY, secondary);
  }

  /**
   * Erases ThreadLocals by nulling out Thread maps.
   */
  static final void eraseThreadLocals(Thread thread) {
    U.putObject(thread, THREADLOCALS, null);
    U.putObject(thread, INHERITABLETHREADLOCALS, null);
  }

  static final void setInheritedAccessControlContext(Thread thread, AccessControlContext acc) {
    U.putOrderedObject(thread, INHERITEDACCESSCONTROLCONTEXT, acc);
  }

  static final void setContextClassLoader(Thread thread, ClassLoader ccl) {
    U.putObject(thread, CCL, ccl);
  }
}
