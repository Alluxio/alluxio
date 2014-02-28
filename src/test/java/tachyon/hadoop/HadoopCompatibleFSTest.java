/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.hadoop;

import java.io.IOException;

//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.hdfs.DistributedFileSystem;
//import org.apache.hadoop.hdfs.MiniDFSCluster;
//import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.junit.After;
import org.junit.Before;
import tachyon.client.TachyonFS;
import tachyon.master.LocalTachyonCluster;

/**
 * Unit tests for <code>tachyon.hadoop.HadoopCompatibleFS</code>.
 */
public class HadoopCompatibleFSTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  //  private MiniDFSCluster mDfsCluster = null;
  //  private DistributedFileSystem mDfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();

    //    Configuration conf = new Configuration();
    //    System.setProperty("fs.default.name", "hdfs://localhost:54310");
    //    mDfsCluster = new MiniDFSCluster();
    //    FileSystem fs = mDfsCluster.getFileSystem();
    //    Assert.assertTrue("Not a HDFS: "+fs.getUri(), fs instanceof DistributedFileSystem);
    //    mDfs = (DistributedFileSystem) fs;
  }
}
