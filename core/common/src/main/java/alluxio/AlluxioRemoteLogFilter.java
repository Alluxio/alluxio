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
  public static final String REMOTE_LOG_MDC_PROCESS_TYPE_KEY = "ProcessType";

  /**
   * @deprecated Option name to configure this {@link AlluxioRemoteLogFilter}
   * in log4j.properties.
   */
  @Deprecated
  public static final String PROCESS_TYPE_OPTION = "ProcessType";

  /** Type of the process generating this log message. */
  private String mProcessType;

  /**
   * @deprecated Gets the option strings.
   * @return option strings as an array
   */
  @Deprecated
  public String[] getOptionStrings() {
    return new String[] {PROCESS_TYPE_OPTION};
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
    if (key.equalsIgnoreCase(PROCESS_TYPE_OPTION)) {
      mProcessType = value;
    }
  }

  /**
   * Sets {@code mProcessType} to be the type of the process generating this log message.
   *
   * Log4j parses log4j.properties, extracting the Java class that corresponds to the filter.
   * In this case, the Java class is {@link AlluxioRemoteLogFilter}.
   * Log4j also extracts option information as a key-value pair, e.g.
   * "ProcessType" : "MASTER". Then log4j invokes the {@link #setProcessType(String)}
   * method to set the value of {@link #mProcessType}.
   *
   * @param processType name of the log appender
   */
  public void setProcessType(String processType) {
    mProcessType = processType;
  }

  /**
   * Retrieves the string representation of process type.
   *
   * This method can potentially be called by log4j reflectively. Even if currently log4j
   * does not call it, it is possible to be called in the future. Therefore, to be
   * consistent with {@link org.apache.log4j.varia.StringMatchFilter}, we should prevent
   * this method from been removed.
   *
   * @return the name of the log appender
   */
  public String getProcessType() {
    return mProcessType;
  }

  @Override
  public int decide(LoggingEvent event) {
    MDC.put(REMOTE_LOG_MDC_PROCESS_TYPE_KEY, mProcessType);
    return ACCEPT;
  }
}
