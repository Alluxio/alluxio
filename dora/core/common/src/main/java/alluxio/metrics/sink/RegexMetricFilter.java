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

package alluxio.metrics.sink;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * A regex metrics filter.
 */
public class RegexMetricFilter implements MetricFilter {
  private static final String SLF4J_KEY_FILTER_REGEX = "filter-regex";

  private final Properties mProperties;
  private final String mRegex;

  /**
   * Creates a new {@link RegexMetricFilter} with a {@link Properties}.
   *
   * @param properties the properties which may contain filter-regex properties
   */
  public RegexMetricFilter(Properties properties) {
    mProperties = properties;
    mRegex = getRegex();
  }

  @Override
  public boolean matches(String name, Metric metric) {
    if (mRegex != null) {
      return name.matches(mRegex);
    } else {
      return true;
    }
  }

  /**
   * Gets the regex of filter.
   *
   * @return the regex of filter set by properties, If it is not set or blank, a null value is
   *         returned.
   */
  private String getRegex() {
    String regex = mProperties.getProperty(SLF4J_KEY_FILTER_REGEX);
    if (StringUtils.isBlank(regex)) {
      regex = null;
    }
    return regex;
  }
}
