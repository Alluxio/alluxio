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

import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A sink which creates a CSV file of the metric values.
 */
@ThreadSafe
public class CsvSink implements Sink {
  private static final Logger LOG = LoggerFactory.getLogger(CsvSink.class);
  private static final int CSV_DEFAULT_PERIOD = 10;
  private static final String CSV_DEFAULT_UNIT = "SECONDS";
  private static final String CSV_DEFAULT_DIR = "/tmp/";

  private static final String CSV_KEY_PERIOD = "period";
  private static final String CSV_KEY_UNIT = "unit";
  private static final String CSV_KEY_DIR = "directory";

  private CsvReporter mReporter;
  private Properties mProperties;
  private final File mDir;

  /**
   * Creates a new {@link CsvSink} with a {@link Properties} and {@link MetricRegistry}.
   *
   * @param properties the properties which may contain polling period, unit and  directory
   *                   properties.
   * @param registry the metric registry to register
   */
  public CsvSink(Properties properties, MetricRegistry registry) {
    mProperties = properties;
    mDir = new File(getPollDir());
    createPollDir(mDir);
    mReporter =
        CsvReporter.forRegistry(registry).formatFor(Locale.US)
            .convertDurationsTo(TimeUnit.MILLISECONDS).convertRatesTo(TimeUnit.SECONDS)
            .build(mDir);
    MetricsSystem.checkMinimalPollingPeriod(getPollUnit(), getPollPeriod());
  }

  @Override
  public void start() {
    mReporter.start(getPollPeriod(), getPollUnit());
  }

  @Override
  public void stop() {
    mReporter.stop();
  }

  @Override
  public void report() {
    mReporter.report();
  }

  /**
   * Gets the directory where the CSV files are created.
   *
   * @return the polling directory set by properties. If it is not set, a default value /tmp/ is
   *         returned.
   */
  private String getPollDir() {
    String pollDir = mProperties.getProperty(CSV_KEY_DIR);
    return pollDir != null ? pollDir : CSV_DEFAULT_DIR;
  }

  /**
   * Gets the polling period.
   *
   * @return the polling period set by properties. If it is not set, a default value 10 is
   *         returned.
   */
  private int getPollPeriod() {
    String period = mProperties.getProperty(CSV_KEY_PERIOD);
    return period != null ? Integer.parseInt(period) : CSV_DEFAULT_PERIOD;
  }

  /**
   * Gets the polling time unit.
   *
   * @return the polling time unit set by properties, If it is not set, a default value SECONDS is
   *         returned.
   */
  private TimeUnit getPollUnit() {
    String unit = mProperties.getProperty(CSV_KEY_UNIT);
    if (unit == null) {
      unit = CSV_DEFAULT_UNIT;
    }
    return TimeUnit.valueOf(unit.toUpperCase());
  }

  /**
   * Create the directory for CSV sink if target directory is not created.
   *
   * @param dir CSV target directory
   */
  private void createPollDir(File dir) {
    if (!dir.exists()) {
      try {
        dir.mkdirs();
      } catch (SecurityException e) {
        LOG.warn("Fail to create directory {} for CSV sink, {}", dir, e.toString());
      }
    }
  }
}
