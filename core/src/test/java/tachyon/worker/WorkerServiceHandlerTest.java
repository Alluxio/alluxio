/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker;

import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.WorkerConf;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;

import org.apache.thrift.TException;

import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.ClientFileInfo;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerServiceHandler;

/**
 * Unit tests for tachyon.WorkerServiceHandler
 */
public class WorkerServiceHandlerTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;
  private WorkerServiceHandler mWorkerServiceHandler = null;
  private TachyonFS mTfs = null;
  private final long WORKER_CAPACITY_BYTES = 10000;
  private final int USER_QUOTA_UNIT_BYTES = 100;
  private final int WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS =
      WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mWorkerServiceHandler = mLocalTachyonCluster.getWorker().getWorkerServiceHandler();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
    mTfs = mLocalTachyonCluster.getClient();
  }

  @Test
  public void evictionTest() throws InvalidPathException, FileAlreadyExistException, IOException,
      FileDoesNotExistException, TException {
    int fileId1 =
        TestUtils.createByteFile(mTfs, "/file1", WriteType.MUST_CACHE,
            (int) WORKER_CAPACITY_BYTES / 3);
    Assert.assertTrue(fileId1 >= 0);
    ClientFileInfo fileInfo1 = mMasterInfo.getClientFileInfo("/file1");
    Assert.assertEquals(100, fileInfo1.inMemoryPercentage);
    int fileId2 =
        TestUtils.createByteFile(mTfs, "/file2", WriteType.MUST_CACHE,
            (int) WORKER_CAPACITY_BYTES / 3);
    Assert.assertTrue(fileId2 >= 0);
    fileInfo1 = mMasterInfo.getClientFileInfo("/file1");
    ClientFileInfo fileInfo2 = mMasterInfo.getClientFileInfo("/file2");
    Assert.assertEquals(100, fileInfo1.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo2.inMemoryPercentage);
    int fileId3 =
        TestUtils.createByteFile(mTfs, "/file3", WriteType.MUST_CACHE,
            (int) WORKER_CAPACITY_BYTES / 2);
    CommonUtils.sleepMs(null, WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    fileInfo1 = mMasterInfo.getClientFileInfo("/file1");
    fileInfo2 = mMasterInfo.getClientFileInfo("/file2");
    ClientFileInfo fileInfo3 = mMasterInfo.getClientFileInfo("/file3");
    Assert.assertTrue(fileId3 >= 0);
    Assert.assertEquals(0, fileInfo1.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo2.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo3.inMemoryPercentage);
  }

  @Test
  public void overCapacityRequestSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES / 10L));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES * 10L));
  }

  @Test
  public void overReturnSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 10));
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 10));
    mWorkerServiceHandler.returnSpace(1, WORKER_CAPACITY_BYTES);
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES));
  }

  @Test
  public void returnSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES));
    mWorkerServiceHandler.returnSpace(1, WORKER_CAPACITY_BYTES);
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES));
    mWorkerServiceHandler.returnSpace(2, WORKER_CAPACITY_BYTES);
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 10));
  }

  @Test
  public void totalOverCapacityRequestSpaceTest() throws TException {
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 2));
    Assert.assertTrue(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 2));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 2));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 2));
  }
}