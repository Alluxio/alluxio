package tachyon.util;

import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public final class ThreadFactoryUtils {
  private ThreadFactoryUtils() {}

  /**
   * Creates a {@link java.util.concurrent.ThreadFactory} that spawns off daemon threads.
   * 
   * @param nameFormat name pattern for each thread. should contain '%d' to distinguish between
   *        threads.
   */
  public static ThreadFactory daemon(final String nameFormat) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build();
  }

  /**
   * Creates a {@link java.util.concurrent.ThreadFactory} that spawns off threads.
   *
   * @param nameFormat name pattern for each thread. should contain '%d' to distinguish between
   *        threads.
   */
  public static ThreadFactory build(final String nameFormat) {
    return new ThreadFactoryBuilder().setDaemon(false).setNameFormat(nameFormat).build();
  }
}
