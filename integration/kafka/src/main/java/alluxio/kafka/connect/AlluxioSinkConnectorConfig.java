/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.kafka.connect;

import alluxio.kafka.connect.format.JsonFormat;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * This class is responsible for reading and storing allxuio connector config parameters.
 */
public class AlluxioSinkConnectorConfig extends AbstractConfig {

  public static final String ALLUXIO_URL = "alluxio.url";
  public static final String TOPICS_DIR = "topics.dir";
  public static final String ROTATION_RECORD_NUM = "rotation.num";
  public static final String ROTATION_TIME_INTERVAL_MS = "rotation.time.interval.ms";
  public static final String ALLUXIO_FORMAT = "alluxio.format";
  public static final int OFFSET_LENGTH = 10;
  public static final String ALLUXIO_CONNECTOR_VERSION = "1.1.0";
  public static final String RETRY_TIME_INTERVAL_MS = "retry.time.interval.ms";
  private static final String ALLUXIO_URL_DOC =
      "alluxio url address, eg. alluxio://localhost:19998";
  private static final String TOPICS_DIR_DOC = "The top level directory name in alluxio";
  private static final String ROTATION_RECORD_NUM_DOC =
      "The record number of alluxio file rotation";
  private static final String ROTATION_TIME_INTERVAL_MS_DOC =
      "The time interval of alluxio file rotation";
  private static final String ALLUXIO_FORMAT_DOC = "The converter format from kafka to alluxio";
  private static final String RETRY_TIME_INTERVAL_MS_DOC =
      "The retry interval in case of transient exception";
  private static ConfigDef sConfig = new ConfigDef()
      .define(ALLUXIO_URL, Type.STRING, "alluxio://localhost:19998", ConfigDef.Importance.HIGH,
          ALLUXIO_URL_DOC)
      .define(TOPICS_DIR, Type.STRING, "topics", ConfigDef.Importance.HIGH, TOPICS_DIR_DOC)
      .define(ROTATION_RECORD_NUM, Type.LONG, "5000", ConfigDef.Importance.HIGH,
          ROTATION_RECORD_NUM_DOC)
      .define(ROTATION_TIME_INTERVAL_MS, Type.LONG, "-1", ConfigDef.Importance.LOW,
          ROTATION_TIME_INTERVAL_MS_DOC)
      .define(ALLUXIO_FORMAT, Type.STRING, JsonFormat.class.getName(), ConfigDef.Importance.HIGH,
          ALLUXIO_FORMAT_DOC)
      .define(RETRY_TIME_INTERVAL_MS, Type.LONG, "5000", ConfigDef.Importance.LOW,
          RETRY_TIME_INTERVAL_MS_DOC);

  /**
   * Gets ConfigDef.
   * @return ConfigDef
   */
  public static ConfigDef getConfig() {
    return sConfig;
  }

  /**
   * AlluxioSinkConnectorConfig Constructor.
   *
   * @param originals the map of parameter key and value
   */
  public AlluxioSinkConnectorConfig(Map<?, ?> originals) {
    super(sConfig, originals);
  }
}
