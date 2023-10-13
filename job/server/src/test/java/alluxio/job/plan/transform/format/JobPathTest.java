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
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import alluxio.client.ReadType;
import alluxio.conf.PropertyKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.mockito.MockedStatic;

public class JobPathTest {

  @Test
  public void testCache() throws Exception {
    try (MockedStatic<JobPath> jobPathMocked = mockStatic(JobPath.class);
         MockedStatic<UserGroupInformation> userGroupInfoMocked =
             mockStatic(UserGroupInformation.class)) {
      jobPathMocked.when(() -> JobPath.fileSystemGet(any(), any()))
          .thenAnswer((p) -> mock(FileSystem.class));
      userGroupInfoMocked.when(UserGroupInformation::getCurrentUser).thenReturn(null);

      Configuration conf = new Configuration();
      JobPath jobPath = new JobPath("foo", "bar", "/baz");
      FileSystem fileSystem = jobPath.getFileSystem(conf);

      verify(JobPath.class, times(1));
      JobPath.fileSystemGet(any(), any());

      assertEquals(fileSystem, jobPath.getFileSystem(conf));
      verify(JobPath.class, times(1));
      JobPath.fileSystemGet(any(), any());

      conf.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT.toString(), ReadType.NO_CACHE.toString());
      FileSystem newFileSystem = jobPath.getFileSystem(conf);
      assertNotEquals(fileSystem, newFileSystem);
      verify(JobPath.class, times(2));
      JobPath.fileSystemGet(any(), any());

      conf.set("foo", "bar");
      assertEquals(newFileSystem, jobPath.getFileSystem(conf));
      verify(JobPath.class, times(2));
      JobPath.fileSystemGet(any(), any());

      jobPath = new JobPath("foo", "bar", "/bar");
      assertEquals(newFileSystem, jobPath.getFileSystem(conf));
      verify(JobPath.class, times(2));
      JobPath.fileSystemGet(any(), any());

      jobPath = new JobPath("foo", "baz", "/bar");
      assertNotEquals(newFileSystem, jobPath.getFileSystem(conf));
      verify(JobPath.class, times(3));
      JobPath.fileSystemGet(any(), any());

      jobPath = new JobPath("bar", "bar", "/bar");
      assertNotEquals(newFileSystem, jobPath.getFileSystem(conf));
      verify(JobPath.class, times(4));
      JobPath.fileSystemGet(any(), any());
    }
  }
}
