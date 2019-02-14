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

package alluxio.job;

import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple dummy job for testing.
 */
@NotThreadSafe
public final class TestJobDefinition
    extends AbstractVoidJobDefinition<TestJobConfig, SerializableVoid> {

  /**
   * Constructs a new {@link TestJobDefinition}.
   */
  public TestJobDefinition() {}

  @Override
  public Class<TestJobConfig> getJobConfigClass() {
    return TestJobConfig.class;
  }

  @Override
  public SerializableVoid runTask(TestJobConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext) throws Exception {
    return null;
  }

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(TestJobConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    return null;
  }
}
