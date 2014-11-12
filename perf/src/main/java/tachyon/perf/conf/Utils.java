package tachyon.perf.conf;

import com.google.common.base.Preconditions;

/**
 * Utils for tachyon.perf.conf package
 */
public class Utils {
  public static boolean getBooleanProperty(String property) {
    return Boolean.valueOf(getProperty(property));
  }

  public static boolean getBooleanProperty(String property, boolean defaultValue) {
    return Boolean.valueOf(getProperty(property, defaultValue + ""));
  }

  public static int getIntProperty(String property) {
    return Integer.valueOf(getProperty(property));
  }

  public static int getIntProperty(String property, int defaultValue) {
    return Integer.valueOf(getProperty(property, defaultValue + ""));
  }

  public static long getLongProperty(String property) {
    return Long.valueOf(getProperty(property));
  }

  public static long getLongProperty(String property, int defaultValue) {
    return Long.valueOf(getProperty(property, defaultValue + ""));
  }

  public static String getProperty(String property) {
    String ret = System.getProperty(property);
    Preconditions.checkArgument(ret != null, property + " is not configured.");
    return ret;
  }

  public static String getProperty(String property, String defaultValue) {
    String ret = System.getProperty(property);
    if (ret == null) {
      ret = defaultValue;
    }
    return ret;
  }
}
