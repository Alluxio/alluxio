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

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

/**
 * AlluxioFormat Interface describes the data format which stored in Alluxio.
 */
public interface AlluxioFormat {

  /**
   * Get File extension name.
   *
   * @return File extension name
   */
  public String getExtension();

  /**
   * Write record to Alluxio file out stream.
   *
   * @param fileStream FileOutStream
   * @param record SinkRecord
   * @throws IOException if write error occurs
   */
  public void writeRecord(FileOutStream fileStream, SinkRecord record) throws IOException;
}
