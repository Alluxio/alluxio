package tachyon.client;

import java.util.Random;

/**
 * Utilities class for Tachyon Client. All methods and variables are static. This class is thread
 * safe.
 */
public final class Utils {
  private static Random sRandom = new Random();

  public static synchronized long getRandomNonNegativeLong() {
    return Math.abs(sRandom.nextLong());
  }

  // Prevent instantiation
  private Utils() {}
}
