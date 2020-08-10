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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JobPath.class, UserGroupInformation.class})
public class JobPathTest {

  @Before
  public void before() throws Exception {
    mockStatic(UserGroupInformation.class);

    when(UserGroupInformation.getCurrentUser()).thenReturn(null);
  }

  @Test
  public void testCache() throws Exception {
    mockStatic(JobPath.class);

    Configuration conf = new Configuration();
    JobPath jobPath = new JobPath("foo", "bar", "/baz");

    when(JobPath.fileSystemGet(any(), any())).thenAnswer((p) -> mock(FileSystem.class));

    FileSystem fileSystem = jobPath.getFileSystem(conf);

    verifyStatic(JobPath.class, times(1));
    JobPath.fileSystemGet(any(), any());

    assertEquals(fileSystem, jobPath.getFileSystem(conf));
    verifyStatic(JobPath.class, times(1));
    JobPath.fileSystemGet(any(), any());

    conf.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT.toString(), ReadType.NO_CACHE.toString());
    FileSystem newFileSystem = jobPath.getFileSystem(conf);
    assertNotEquals(fileSystem, newFileSystem);
    verifyStatic(JobPath.class, times(2));
    JobPath.fileSystemGet(any(), any());

    conf.set("foo", "bar");
    assertEquals(newFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(JobPath.class, times(2));
    JobPath.fileSystemGet(any(), any());

    jobPath = new JobPath("foo", "bar", "/bar");
    assertEquals(newFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(JobPath.class, times(2));
    JobPath.fileSystemGet(any(), any());

    jobPath = new JobPath("foo", "baz", "/bar");
    assertNotEquals(newFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(JobPath.class, times(3));
    JobPath.fileSystemGet(any(), any());

    jobPath = new JobPath("bar", "bar", "/bar");
    assertNotEquals(newFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(JobPath.class, times(4));
    JobPath.fileSystemGet(any(), any());
  }
}
