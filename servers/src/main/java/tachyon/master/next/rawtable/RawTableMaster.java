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

package tachyon.master.next.rawtable;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.next.Master;
import tachyon.master.next.PeriodicTask;
import tachyon.master.next.filesystem.FileSystemMaster;
import tachyon.master.next.rawtable.meta.RawTables;
import tachyon.thrift.TachyonException;

public class RawTableMaster implements Master {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonConf mTachyonConf;
  private final long mMaxTableMetadataBytes;

  private final FileSystemMaster mFileSystemMaster;
  private final RawTables mRawTables = new RawTables();

  public RawTableMaster(TachyonConf tachyonConf, FileSystemMaster fileSystemMaster) {
    mTachyonConf = tachyonConf;
    mMaxTableMetadataBytes = mTachyonConf.getBytes(Constants.MAX_TABLE_METADATA_BYTE);
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public TProcessor getProcessor() {
    return null;
  }

  @Override
  public String getProcessorName() {
    return "RawTableMaster";
  }

  public List<PeriodicTask> getPeriodicTaskList() {
    // TODO
    return Collections.emptyList();
  }

  /**
   * Validate size of metadata is smaller than the configured maximum size. This should be called
   * whenever a metadata wants to be set.
   *
   * @param metadata the metadata to be validated
   * @throws TachyonException if the metadata is too large
   */
  private void validateMetadataSize(ByteBuffer metadata) throws TachyonException {
    if (metadata.limit() - metadata.position() >= mMaxTableMetadataBytes) {
      throw new TachyonException("Too big table metadata: " + metadata.toString());
    }
  }
}
