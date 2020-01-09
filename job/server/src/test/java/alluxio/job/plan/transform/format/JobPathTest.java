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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
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
@PrepareForTest({FileSystem.class, UserGroupInformation.class})
public class JobPathTest {

  @Before
  public void before() throws Exception {
    mockStatic(UserGroupInformation.class);

    when(UserGroupInformation.getCurrentUser()).thenReturn(null);
  }

  @Test
  public void testCache() throws Exception {
    mockStatic(FileSystem.class);

    Configuration conf = new Configuration();
    JobPath jobPath = new JobPath("foo", "bar", "/baz");

    FileSystem mockFileSystem = mock(FileSystem.class);

    when(FileSystem.get(eq(jobPath.toUri()), any())).thenReturn(mockFileSystem);

    assertEquals(mockFileSystem, jobPath.getFileSystem(conf));

    verifyStatic(times(1));
    FileSystem.get(eq(jobPath.toUri()), any());

    assertEquals(mockFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(times(1));
    FileSystem.get(eq(jobPath.toUri()), any());

    conf.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT.toString(), ReadType.NO_CACHE.toString());
    assertEquals(mockFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(times(2));
    FileSystem.get(eq(jobPath.toUri()), any());

    conf.set("foo", "bar");
    assertEquals(mockFileSystem, jobPath.getFileSystem(conf));
    verifyStatic(times(2));
    FileSystem.get(eq(jobPath.toUri()), any());
  }
}
