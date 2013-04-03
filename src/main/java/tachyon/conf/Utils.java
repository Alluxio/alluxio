package tachyon.conf;

import org.apache.log4j.Logger;

import tachyon.CommonUtils;

/**
 * Utils for tachyon.conf package.
 */
class Utils {
  private static final Logger LOG = Logger.getLogger("");

  public static String getProperty(String property) {
    String ret = System.getProperty(property);
    if (ret == null) {
      CommonUtils.illegalArgumentException(property + " is not configured.");
    } else {
      LOG.debug(property + " : " + ret);
    }
    return ret;
  }

  public static String getProperty(String property, String defaultValue) {
    String ret = System.getProperty(property);
    String msg = "";
    if (ret == null) {
      ret = defaultValue;
      msg = " uses the default value";
    }
    LOG.debug(property + msg + " : " + ret);
    return ret;
  }

  public static int getIntProperty(String property) {
    return Integer.valueOf(getProperty(property));
  }

  public static int getIntProperty(String property, int defaultValue) {
    return Integer.valueOf(getProperty(property, defaultValue + ""));
  }

  public static boolean getBooleanProperty(String property) {
    return Boolean.valueOf(getProperty(property));
  }

  public static boolean getBooleanProperty(String property, boolean defaultValue) {
    return Boolean.valueOf(getProperty(property, defaultValue + ""));
  }
}
