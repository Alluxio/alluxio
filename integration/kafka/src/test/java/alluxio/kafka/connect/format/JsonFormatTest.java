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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.master.LocalAlluxioCluster;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link JsonFormat} class..
 */
public class JsonFormatTest {

  /**
   * Tests for writing record to Alluxio file.
   */
  @Test
  public void writeRecordTest() throws Exception {
    LocalAlluxioCluster cluster = new LocalAlluxioCluster(Constants.GB, 128 * Constants.MB);
    cluster.start();

    FileSystem client = cluster.getClient();
    AlluxioURI testURI = new AlluxioURI("/testFile");
    FileOutStream outStream = client.createFile(testURI);
    final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
        .field("name", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .build();
    final Struct record = new Struct(valueSchema)
        .put("name", "Lily")
        .put("id", 1);
    SinkRecord sinkRecord = new SinkRecord("topic_test", 0, null, null, valueSchema, record, 0);
    JsonFormat jsonFormat = new JsonFormat();
    jsonFormat.writeRecord(outStream, sinkRecord);
    outStream.close();

    Assert.assertNotNull(client.getStatus(testURI));
    Assert.assertTrue(client.getStatus(testURI).getLength() > 0);
  }
}
