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

package alluxio.kafka.connect.format;

import alluxio.client.file.FileOutStream;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This Class converts kafka connect data to alluxio in Json format.
 */
public class JsonFormat implements AlluxioFormat {

  private static final Logger LOG = LoggerFactory.getLogger(JsonFormat.class);
  private final String mExtension = ".json";
  private final JsonConverter mJsonConverter = new JsonConverter();

  /**
   * JsonFormat Constructor, schema is disabled by default.
   */
  public JsonFormat() {
    Map<String, Object> configure = new HashMap<>();
    configure.put("schemas.enable", false);
    mJsonConverter.configure(configure, false);
  }

  @Override
  public void writeRecord(FileOutStream fileStream, SinkRecord record)
      throws IOException {
    if (record.value() != null) {
      LOG.info(record.value().toString());
    }
    if (record.valueSchema() != null) {
      LOG.info(record.valueSchema().toString());
    }
    byte[] serializedResult =
        mJsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    fileStream.write(serializedResult);
  }

  @Override
  public String getExtension() {
    return mExtension;
  }
}
