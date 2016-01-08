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

package tachyon.client.table;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.conf.TachyonConf;

/**
 * Integration tests for {@link RawColumn}.
 */
// TODO(calvin): Move this into TachyonRawTablesIntegrationTest
public class RawColumnIntegrationTest {

  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(10000, 1000, Constants.GB);
  private FileSystem mFileSystem = null;
  private TachyonRawTables mTachyonRawTables = null;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalTachyonClusterResource.get().getClient();
    mTachyonRawTables = TachyonRawTables.TachyonRawTablesFactory.get();
  }

  @Test
  public void basicTest() throws Exception {
    TachyonConf conf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
    int maxCols = conf.getInt(Constants.MAX_COLUMNS);

    RawTable table = mTachyonRawTables.create(new TachyonURI("/table"), maxCols / 10);

    for (int col = 0; col < maxCols / 10; col ++) {
      RawColumn column = table.getColumn(col);
      for (int pid = 0; pid < 5; pid ++) {
        // Create an empty partition
        mTachyonRawTables.createPartition(column, pid).close();
        TachyonURI file = mTachyonRawTables.getPartitionUri(column, pid);
        URIStatus partitionInfo = mFileSystem.getStatus(file);
        Assert.assertEquals("/table" + TachyonURI.SEPARATOR + Constants.MASTER_COLUMN_FILE_PREFIX
            + col + TachyonURI.SEPARATOR + pid, partitionInfo.getPath());
      }
      Assert.assertEquals(5, mTachyonRawTables.getPartitionCount(column));
    }
  }
}
