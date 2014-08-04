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

import java.io.IOException;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.StorageId;
import tachyon.StorageLevelAlias;
import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerDirInfo;
import tachyon.util.CommonUtils;

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
  public void overCapacityRequestSpaceTest() throws TException, IOException {
    WorkerDirInfo actual = mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES / 10L);
    Assert.assertEquals(actual.getStorageId(),
        StorageId.getStorageId(0, StorageLevelAlias.MEM.getValue(), 0));
    TachyonException exception = null;
    try {
      actual = mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES);
    } catch (TachyonException e) {
      exception = e;
    }
    Assert.assertEquals(exception, new TachyonException("no block can be evicted in current tier!"));
  }

  @Test
  public void overReturnSpaceTest() throws TException, IOException {
    WorkerDirInfo actual = mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES / 10L);
    Assert.assertEquals(actual.getStorageId(),
        StorageId.getStorageId(0, StorageLevelAlias.MEM.getValue(), 0));
    mWorkerServiceHandler.returnSpace(actual.getStorageId(), 1, WORKER_CAPACITY_BYTES);
    TachyonException exception = null;
    try {
      actual = mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES);
    } catch (TachyonException e) {
      exception = e;
    }
    Assert.assertEquals(exception, new TachyonException("no block can be evicted in current tier!"));
  }

  @Test
  public void returnSpaceTest() throws TException, IOException {
    WorkerDirInfo actual0 = mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES);
    Assert.assertEquals(actual0.getStorageId(),
        StorageId.getStorageId(0, StorageLevelAlias.MEM.getValue(), 0));
    TachyonException exception = null;
    try {
      WorkerDirInfo actual1 = mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES);
    } catch (TachyonException e) {
      exception = e;
    }
    Assert.assertEquals(exception, new TachyonException("no block can be evicted in current tier!"));
    mWorkerServiceHandler.returnSpace(actual0.getStorageId(), 1, WORKER_CAPACITY_BYTES);
    WorkerDirInfo actual2 = mWorkerServiceHandler.requestSpace(1L, WORKER_CAPACITY_BYTES);
    Assert.assertEquals(actual2.getStorageId(),
        StorageId.getStorageId(0, StorageLevelAlias.MEM.getValue(), 0));
    mWorkerServiceHandler.returnSpace(actual2.getStorageId(), 2, WORKER_CAPACITY_BYTES);
    try {
      WorkerDirInfo actual3 = mWorkerServiceHandler.requestSpace(2L, WORKER_CAPACITY_BYTES / 10);
    } catch (TachyonException e) {
      exception = e;
    }
    Assert.assertEquals(exception, new TachyonException("no block can be evicted in current tier!"));
  }

  @Test
  public void totalOverCapacityRequestSpaceTest() throws TException, IOException {
    WorkerDirInfo actual = mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 2);
    Assert.assertEquals(actual.getStorageId(),
        StorageId.getStorageId(0, StorageLevelAlias.MEM.getValue(), 0));
    TachyonException exception = null;
    try {
      actual = mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES + 1);
    } catch (TachyonException e) {
      exception = e;
    }
    Assert.assertEquals(exception, new TachyonException("no dir is allocated!"));
    actual = mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 2);
    Assert.assertEquals(actual.getStorageId(),
        StorageId.getStorageId(0, StorageLevelAlias.MEM.getValue(), 0));
    try {
      actual = mWorkerServiceHandler.requestSpace(1, WORKER_CAPACITY_BYTES / 2);
    } catch (TachyonException e) {
      exception = e;
    }
    Assert.assertEquals(exception, new TachyonException("no block can be evicted in current tier!"));
    try {
      actual = mWorkerServiceHandler.requestSpace(2, WORKER_CAPACITY_BYTES / 2);
    } catch (TachyonException e) {
      exception = e;
    }
    Assert.assertEquals(exception, new TachyonException("no block can be evicted in current tier!"));
  }
}