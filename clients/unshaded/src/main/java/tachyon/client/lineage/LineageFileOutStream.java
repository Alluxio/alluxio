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

package tachyon.client.lineage;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.WriteType;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.options.OutStreamOptions;

/**
 * A stream API to write a file when lineage is enabled. It supports asynchronous persistence of the
 * data to the under storage system.
 */
@PublicApi
public class LineageFileOutStream extends FileOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Creates a new file output stream when lineage is enabled.
   *
   * @param path the path of the file
   * @param options the set of options specific to this operation
   * @throws IOException if an I/O error occurs
   */
  public LineageFileOutStream(TachyonURI path, OutStreamOptions options) throws IOException {
    super(path, updateOutStreamOptions(options));
  }

  private static OutStreamOptions updateOutStreamOptions(OutStreamOptions options) {
    return options.setWriteType(WriteType.ASYNC_THROUGH);
  }

  @Override
  protected void scheduleAsyncPersist() throws IOException {
    // do nothing, the scheduling is handled by the lineage master
    return;
  }
}
