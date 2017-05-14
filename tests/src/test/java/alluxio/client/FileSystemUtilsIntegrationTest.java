/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link alluxio.client.file.FileSystemUtils}.
 */
public class FileSystemUtilsIntegrationTest extends BaseIntegrationTest {
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, USER_QUOTA_UNIT_BYTES)
          .build();
  private static CreateFileOptions sWriteBoth;
  private static FileSystem sFileSystem = null;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
    sWriteBoth = CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH);
  }

  @Test
  public void waitCompletedTest1() throws IOException, AlluxioException, InterruptedException {
    final String uniqPath = PathUtils.uniqPath();
    final int numWrites = 4; // random value chosen through a fair dice roll :P
    final AlluxioURI uri = new AlluxioURI(uniqPath);

    final Runnable writer = new Runnable() {
      @Override
      public void run() {
        try {
          FileOutStream os = sFileSystem.createFile(uri, sWriteBoth);
          boolean completed = sFileSystem.getStatus(uri).isCompleted();
          Assert.assertFalse(completed);
          for (int i = 0; i < numWrites; i++) {
            os.write(42);
            CommonUtils.sleepMs(200);
          }
          os.close();
          completed = sFileSystem.getStatus(uri).isCompleted();
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
          boolean completed = FileSystemUtils.waitCompleted(sFileSystem, uri);
          Assert.assertTrue(completed);
          completed = sFileSystem.getStatus(uri).isCompleted();
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
  public void waitCompletedTest2() throws IOException, AlluxioException, InterruptedException {
    final String uniqPath = PathUtils.uniqPath();
    final int numWrites = 4; // random value chosen through a fair dice roll :P
    final AlluxioURI uri = new AlluxioURI(uniqPath);

    final Runnable writer = new Runnable() {
      @Override
      public void run() {
        try {
          FileOutStream os = sFileSystem.createFile(uri, sWriteBoth);
          boolean completed = sFileSystem.getStatus(uri).isCompleted();
          Assert.assertFalse(completed);
          // four writes that will take > 600ms due to the sleeps
          for (int i = 0; i < numWrites; i++) {
            os.write(42);
            CommonUtils.sleepMs(200);
          }
          os.close();
          completed = sFileSystem.getStatus(uri).isCompleted();
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
          // set the slow default polling period to a more sensible value, in order
          // to speed up the tests artificial waiting times
          String original = Configuration.get(PropertyKey.USER_FILE_WAITCOMPLETED_POLL_MS);
          Configuration.set(PropertyKey.USER_FILE_WAITCOMPLETED_POLL_MS, "100");
          try {
            // The write will take at most 600ms I am waiting for at most 400ms - epsilon.
            boolean completed = FileSystemUtils.waitCompleted(sFileSystem, uri, 300,
                TimeUnit.MILLISECONDS);
            Assert.assertFalse(completed);
            completed = sFileSystem.getStatus(uri).isCompleted();
            Assert.assertFalse(completed);
          } finally {
            Configuration.set(PropertyKey.USER_FILE_WAITCOMPLETED_POLL_MS, original);
          }
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
