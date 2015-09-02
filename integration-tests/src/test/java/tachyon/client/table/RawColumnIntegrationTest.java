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

import java.io.IOException;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFile;
import tachyon.client.next.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.master.next.LocalTachyonCluster;

/**
 * Integration tests for tachyon.client.RawColumn.
 */
public class RawColumnIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFileSystem mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  // TODO: renable when there is a raw table client.
/*
  @Test
  public void basicTest() throws IOException, TException {
    TachyonConf conf = mLocalTachyonCluster.getMasterTachyonConf();
    int maxCols = conf.getInt(Constants.MAX_COLUMNS);

    int fileId = mTfs.createRawTable(new TachyonURI("/table"), maxCols / 10);
    RawTable table = mTfs.getRawTable(fileId);

    for (int col = 0; col < maxCols / 10; col ++) {
      RawColumn column = table.getRawColumn(col);
      for (int pid = 0; pid < 5; pid ++) {
        Assert.assertTrue(column.createPartition(pid));
        TachyonFile file = column.getPartition(pid);
        Assert.assertEquals("/table" + TachyonURI.SEPARATOR + Constants.MASTER_COLUMN_FILE_PREFIX
            + col + TachyonURI.SEPARATOR + pid, file.getPath());
      }
      Assert.assertEquals(5, column.partitions());
    }
  }
*/
  
  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(10000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }
}
