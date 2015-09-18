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

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientOptions;
import tachyon.client.file.FileOutStream;

@PublicApi
public class LineageFileOutStream extends FileOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private LineageContext mContext;

  public LineageFileOutStream(long fileId, ClientOptions options) throws IOException {
    super(fileId, updateClientOptions(options));

    mContext = LineageContext.INSTANCE;
  }

  private static ClientOptions updateClientOptions(ClientOptions options) {
    // TODO(yupeng) should we set options to async automatically?
    Preconditions.checkState(options.getUnderStorageType().isAsyncPersist(),
        "the option should be async persist for lineage");
    return options;
  }

  @Override
  public void close() throws IOException {
    if (isClosed()) {
      return;
    }

    LineageMasterClient masterClient = mContext.acquireMasterClient();


  }
}
