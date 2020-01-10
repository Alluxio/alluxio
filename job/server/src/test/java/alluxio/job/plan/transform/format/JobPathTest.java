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

package alluxio.job.plan.transform.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.client.ReadType;
import alluxio.conf.PropertyKey;

import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class, UserGroupInformation.class})
public class JobPathTest {

  @Before
  public void before() throws Exception {
    mockStatic(UserGroupInformation.class);
    mockStatic(FileSystem.class);

    when(UserGroupInformation.getCurrentUser()).thenReturn(null);
    when(FileSystem.get(any(), any())).thenAnswer((p) -> mock(FileSystem.class));
  }

  @Test
  public void testCache() throws Exception {
    Configuration conf = new Configuration();
    JobPath jobPath = new JobPath("foo", "bar", "/baz");

    FileSystem fileSystem = jobPath.getFileSystem(conf);

    verifyStatic(times(1));
    FileSystem.get(any(), any());

    assertEquals(fileSystem, jobPath.getFileSystem(conf));
    verifyStatic(times(1));
    FileSystem.get(any(), any());

    conf.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT.toString(), ReadType.NO_CACHE.toString());
    FileSystem newFileSystem = jobPath.getFileSystem(conf);
    assertNotEquals(fileSystem, newFileSystem);
    verifyStatic(times(2));
    FileSystem.get(any(), any());

    conf.set("foo", "bar");
    assertEquals(newFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(times(2));
    FileSystem.get(any(), any());

    jobPath = new JobPath("foo", "bar", "/bar");
    assertEquals(newFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(times(2));
    FileSystem.get(any(), any());

    jobPath = new JobPath("foo", "baz", "/bar");
    assertNotEquals(newFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(times(3));
    FileSystem.get(any(), any());

    jobPath = new JobPath("bar", "bar", "/bar");
    assertNotEquals(newFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(times(4));
    FileSystem.get(any(), any());
  }

  @Test
  public void testMultithreadedCache() throws Exception {
    int iterations = 5;

    ExecutorService executorService = Executors.newFixedThreadPool(10);

    ConcurrentLinkedQueue<FileSystem> results = new ConcurrentLinkedQueue<>();

    CountDownLatch countDownLatch = new CountDownLatch(1);

    Runnable runnable = () -> {
      try {
        countDownLatch.await();
      } catch (InterruptedException e) {
        return;
      }
      Configuration conf = new Configuration();
      JobPath jobPath = new JobPath("foo", "bar", "/baz");
      try {
        results.add(jobPath.getFileSystem(conf));
      } catch (Exception e) {
        // ignore since the test will fail naturally as there won't be enough items in results
      }
    };

    ArrayList<Future> futures = Lists.newArrayList();

    for (int i = 0; i < iterations; i++) {
      futures.add(executorService.submit(runnable));
    }
    countDownLatch.countDown();

    for (Future future : futures) {
      future.get();
    }

    assertEquals(iterations, results.size());

    FileSystem first = null;
    for (FileSystem result : results) {
      if (first == null) {
        first = result;
        continue;
      }
      assertEquals(first, result);
    }
  }
}
