package tachyon.conf;

import org.apache.log4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

/**
 * Utils for tachyon.conf package.
 */
class Utils {
  private static final Logger LOG = Logger.getLogger("");
  private static final CharMatcher LIST_SPLITTER_MATCHER = CharMatcher.is(',').or(
      CharMatcher.WHITESPACE);
  private static final Splitter LIST_SPLITTER = Splitter.on(LIST_SPLITTER_MATCHER)
      .omitEmptyStrings().trimResults();

  public static boolean getBooleanProperty(String property) {
    return Boolean.valueOf(getProperty(property));
  }

  public static boolean getBooleanProperty(String property, boolean defaultValue) {
    return Boolean.valueOf(getProperty(property, defaultValue + ""));
  }

  public static <T extends Enum<T>> T getEnumProperty(String property, T defaultValue) {
    final String val = getProperty(property, null);
    return null == val ? defaultValue
        : Enum.valueOf(defaultValue.getDeclaringClass(), val);
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

  public static ImmutableList<String> getListProperty(String property,
      ImmutableList<String> defaultValue) {
    final String strList = getProperty(property, null);
    if (strList == null) {
      return defaultValue;
    } else {
      return ImmutableList.copyOf(LIST_SPLITTER.split(strList));
    }
  }

  public static String getProperty(String property) {
    String ret = System.getProperty(property);
    Preconditions.checkArgument(ret != null, property + " is not configured.");
    LOG.debug(property + " : " + ret);
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

}
