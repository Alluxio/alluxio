/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio;

import org.apache.log4j.MDC;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import java.lang.Deprecated;

/**
 * Thin filter to add MDC information to {@link LoggingEvent}. Remote log server can read
 * {@link org.apache.log4j.spi.LoggingEvent} off {@link java.net.Socket} and retrieve the
 * MDC information. In this case the MDC information is the name of the log4j appender.
 * Remote log server then uses this appender to log messages. The implementation of this
 * class is similar to that of {@link org.apache.log4j.varia.StringMatchFilter}.
 */
public class AlluxioRemoteLogFilter extends Filter {
  /** Name (Key) of the MDC info. */
  public static final String REMOTE_LOG_MDC_APPENDER_NAME_KEY = "appender";

  /**
   * @deprecated Option name to configure this {@link AlluxioRemoteLogFilter}
   * in log4j.properties.
   */
  @Deprecated
  public static final String APPENDER_NAME_OPTION = "AppenderName";

  /** Name of the log appender. */
  private String mAppenderName;

  /**
   * @deprecated Gets the option strings.
   * @return option strings as an array
   */
  @Deprecated
  public String[] getOptionStrings() {
    return new String[] {APPENDER_NAME_OPTION};
  }

  /**
   * @deprecated Sets option value use key=value format. The log4j.properties file uses this
   * to set options. See the log4j.properties for more details.
   *
   * @param key key (name) of the option
   * @param value value of the option
   */
  @Deprecated
  public void setOption(String key, String value) {
    if (key.equalsIgnoreCase(APPENDER_NAME_OPTION)) {
      mAppenderName = value;
    }
  }

  /**
   * Sets the name of the log appender.
   *
   * Log4j parses log4j.properties, extracting the Java class that corresponds to the filter.
   * In this case, the Java class is {@link AlluxioRemoteLogFilter}.
   * Log4j also extracts option information as a key-value pair, e.g.
   * "AppenderName" : "MASTER_LOG". Then log4j invokes the {@link #setAppenderName(String)}
   * method to set the value of {@link #mAppenderName}.
   *
   * @param appenderName name of the log appender
   */
  public void setAppenderName(String appenderName) {
    mAppenderName = appenderName;
  }

  /**
   * @return the name of the log appender
   */
  public String getAppenderName() {
    return mAppenderName;
  }

  @Override
  public int decide(LoggingEvent event) {
    MDC.put(REMOTE_LOG_MDC_APPENDER_NAME_KEY, mAppenderName);
    return ACCEPT;
  }
}
