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

package tachyon.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.TachyonFileSystemUtils;
import tachyon.client.file.options.GetInfoOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.util.CommonUtils;
import tachyon.util.io.PathUtils;

/**
 * Tests for {@link tachyon.client.file.TachyonFileSystemUtils}.
 */
public class TachyonFileSystemUtilsIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 2 * Constants.MB;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.MB,
          Constants.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES));
  private static OutStreamOptions sWriteBoth;
  private static TachyonFileSystem sTfs = null;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sTfs = sLocalTachyonClusterResource.get().getClient();
    TachyonConf conf = sLocalTachyonClusterResource.get().getMasterTachyonConf();
    sWriteBoth =  StreamOptionUtils.getOutStreamOptionsWriteBoth(conf);
  }

  @Test
  public void waitCompletedTest1() throws IOException, TachyonException, InterruptedException {
    final String uniqPath = PathUtils.uniqPath();
    final int numWrites = 4; // random value chosen through a fair dice roll :P
    final TachyonURI uri = new TachyonURI(uniqPath);

    final Runnable writer = new Runnable() {
      @Override
      public void run() {
        FileOutStream os = null;
        try {
          os = sTfs.getOutStream(uri, sWriteBoth);
          final TachyonFile file = sTfs.open(uri);
          boolean completed =
              sTfs.getInfo(file, GetInfoOptions.defaults()).isCompleted();
          Assert.assertFalse(completed);
          for (int i = 0; i < numWrites; i++) {
            os.write(42);
            CommonUtils.sleepMs(200);
          }
          os.close();
          completed = sTfs.getInfo(file, GetInfoOptions.defaults()).isCompleted();
          Assert.assertTrue(completed);
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
      }
    };

    final Runnable waiter = new Runnable() {
      @Override
      public void run() {
        try {
          boolean completed = TachyonFileSystemUtils.waitCompleted(sTfs, uri);
          Assert.assertTrue(completed);
          final TachyonFile file = sTfs.open(uri);
          completed =
              sTfs.getInfo(file, GetInfoOptions.defaults()).isCompleted();
          Assert.assertTrue(completed);
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail(e.getMessage());
        }
      }
    };

    final Thread waitingThread = new Thread(waiter);
    waitingThread.start();

    final Thread writingThread = new Thread(writer);
    writingThread.start();

    waitingThread.join();
    writingThread.join();
  }

  @Test
  public void waitCompletedTest2() throws IOException, TachyonException, InterruptedException {
    final String uniqPath = PathUtils.uniqPath();
    final int numWrites = 4; // random value chosen through a fair dice roll :P
    final TachyonURI uri = new TachyonURI(uniqPath);

    final Runnable writer = new Runnable() {
      @Override
      public void run() {
        FileOutStream os = null;
        try {
          os = sTfs.getOutStream(uri, sWriteBoth);
          final TachyonFile file = sTfs.open(uri);
          boolean completed =
              sTfs.getInfo(file, GetInfoOptions.defaults()).isCompleted();
          Assert.assertFalse(completed);
          // four writes that will take > 600ms due to the sleeps
          for (int i = 0; i < numWrites; i++) {
            os.write(42);
            CommonUtils.sleepMs(200);
          }
          os.close();
          completed = sTfs.getInfo(file, GetInfoOptions.defaults()).isCompleted();
          Assert.assertTrue(completed);
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
      }
    };

    final Runnable waiter = new Runnable() {
      @Override
      public void run() {
        try {
          final TachyonConf conf = ClientContext.getConf();
          // set the slow default polling period to a more sensible value, in order
          // to speed up the tests artificial waiting times
          conf.set(Constants.USER_FILE_WAITCOMPLETED_POLL_MS, "100");
          // The write will take at most 600ms I am waiting for at most 400ms - epsilon.
          boolean completed = TachyonFileSystemUtils.waitCompleted(sTfs, uri, 300,
              TimeUnit.MILLISECONDS);
          Assert.assertFalse(completed);
          final TachyonFile file = sTfs.open(uri);
          completed = sTfs.getInfo(file, GetInfoOptions.defaults()).isCompleted();
          Assert.assertFalse(completed);
          ClientContext.reset();
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail(e.getMessage());
        }
      }
    };

    final Thread waitingThread = new Thread(waiter);
    waitingThread.start();

    final Thread writingThread = new Thread(writer);
    writingThread.start();

    waitingThread.join();
    writingThread.join();
  }
}
