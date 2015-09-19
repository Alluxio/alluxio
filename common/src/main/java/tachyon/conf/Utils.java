/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

/**
 * Utils for tachyon.conf package.
 */
final class Utils {
  private static final Logger LOG = LoggerFactory.getLogger("");
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
    return val == null ? defaultValue : Enum.valueOf(defaultValue.getDeclaringClass(), val);
  }

  public static int getIntProperty(String property) {
    return Integer.valueOf(getProperty(property));
  }

  public static int getIntProperty(String property, int defaultValue) {
    return Integer.valueOf(getProperty(property, defaultValue + ""));
  }

  public static Integer getIntegerProperty(String property, Integer defaultValue) {
    try {
      return Integer.valueOf(getProperty(property, null));
    } catch (NumberFormatException e) {
      return defaultValue;
    }
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
    LOG.debug("{} : {}", property, ret);
    return ret;
  }

  public static String getProperty(String property, String defaultValue) {
    String ret = System.getProperty(property);
    String msg = "";
    if (ret == null) {
      ret = defaultValue;
      msg = " uses the default value";
    }
    LOG.debug("{} {} : {}", property, msg, ret);
    return ret;
  }

  private Utils() {} // prevent instantiation
}
