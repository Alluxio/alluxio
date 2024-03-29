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

package alluxio.job.plan;

import static org.mockito.Mockito.mock;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.underfs.UfsManager;
import alluxio.wire.WorkerIdentityTestUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import org.junit.Before;

import java.util.List;

public abstract class SelectExecutorsTest {

  public static final WorkerInfo JOB_WORKER_0 =
      new WorkerInfo()
          .setIdentity(WorkerIdentityTestUtils.randomLegacyId())
          .setAddress(new WorkerNetAddress().setHost("host0"));
  public static final WorkerInfo JOB_WORKER_1 =
      new WorkerInfo()
          .setIdentity(WorkerIdentityTestUtils.randomLegacyId())
          .setAddress(new WorkerNetAddress().setHost("host1"));
  public static final WorkerInfo JOB_WORKER_2 =
      new WorkerInfo()
          .setIdentity(WorkerIdentityTestUtils.randomLegacyId())
          .setAddress(new WorkerNetAddress().setHost("host2"));
  public static final WorkerInfo JOB_WORKER_3 =
      new WorkerInfo()
          .setIdentity(WorkerIdentityTestUtils.randomLegacyId())
          .setAddress(new WorkerNetAddress().setHost("host3"));
  public static final List<WorkerInfo> JOB_WORKERS =
      ImmutableList.of(JOB_WORKER_0, JOB_WORKER_1, JOB_WORKER_2, JOB_WORKER_3);

  protected FileSystem mMockFileSystem;
  protected FileSystemContext mMockFileSystemContext;
  protected UfsManager mMockUfsManager;

  @Before
  public void before() throws Exception {
    mMockFileSystemContext = mock(FileSystemContext.class);
    mMockFileSystem = mock(FileSystem.class);
    mMockUfsManager = mock(UfsManager.class);
  }
}
