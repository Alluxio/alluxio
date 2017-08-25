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

/**
 * Thin filter to add MDC information to {@link LoggingEvent}. Remote log server can read
 * {@link org.apache.log4j.spi.LoggingEvent} off {@link java.net.Socket} and retrieve the
 * MDC information. In this case the MDC information is the name of the log4j appender.
 * Remote log server then uses this appender to log messages. The implementation of this
 * class is similar to that of {@link org.apache.log4j.varia.StringMatchFilter}.
 */
public class AlluxioRemoteLogFilter extends Filter {
  /**
   * Name (Key) of the MDC info.
   */
  public static final String REMOTE_LOG_MDC_APPENDER_NAME_KEY = "appender";
  /**
   * Option name to configure this {@link AlluxioRemoteLogFilter} in log4j.properties.
   */
  public static final String APPENDER_NAME_OPTION = "AppenderName";

  /**
   * Name of the log appender.
   */
  String mAppenderName;

  public String[] getAppenderStrings() {
    return new String[] {APPENDER_NAME_OPTION};
  }

  public void setOption(String key, String value) {
    if (key.equalsIgnoreCase("AppenderName")) {
      mAppenderName = value;
    }
  }

  public void setAppenderName(String appenderName) {
    mAppenderName = appenderName;
  }

  public String getAppenderName() {
    return mAppenderName;
  }

  @Override
  public int decide(LoggingEvent event) {
    MDC.put(REMOTE_LOG_MDC_APPENDER_NAME_KEY, mAppenderName);
    return ACCEPT;
  }
}
