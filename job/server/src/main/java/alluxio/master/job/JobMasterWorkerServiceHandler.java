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

package alluxio.master.job;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.JobHeartbeatTOptions;
import alluxio.thrift.JobHeartbeatTResponse;
import alluxio.thrift.JobMasterWorkerService.Iface;
import alluxio.thrift.RegisterJobWorkerTOptions;
import alluxio.thrift.RegisterJobWorkerTResponse;
import alluxio.thrift.TaskInfo;
import alluxio.thrift.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a Thrift handler for job master RPCs invoked by a job service worker.
 */
@ThreadSafe
public final class JobMasterWorkerServiceHandler implements Iface {
  private static final Logger LOG = LoggerFactory.getLogger(JobMasterWorkerServiceHandler.class);
  private final JobMaster mJobMaster;

  /**
   * Creates a new instance of {@link JobMasterWorkerServiceHandler}.
   *
   * @param JobMaster the {@link JobMaster} that the handler uses internally
   */
  JobMasterWorkerServiceHandler(JobMaster JobMaster) {
    mJobMaster = Preconditions.checkNotNull(JobMaster);
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.JOB_MASTER_WORKER_SERVICE_VERSION);
  }

  @Override
  public RegisterJobWorkerTResponse registerJobWorker(final WorkerNetAddress workerNetAddress,
      RegisterJobWorkerTOptions options) throws TException {
    return RpcUtils.call(LOG, (RpcUtils.RpcCallable<RegisterJobWorkerTResponse>) () ->
        new RegisterJobWorkerTResponse(
            mJobMaster.registerWorker(null)),
        "RegisterJobWorker", "workerNetAddress=%s, options=%s", workerNetAddress, options
    );
  }

  @Override
  public synchronized JobHeartbeatTResponse heartbeat(final long workerId,
      final List<TaskInfo> taskInfoList, JobHeartbeatTOptions options) throws TException {
    return RpcUtils.call(LOG, (RpcUtils.RpcCallable<JobHeartbeatTResponse>) () -> {
      List<alluxio.job.wire.TaskInfo> wireTaskInfoList = Lists.newArrayList();
      for (TaskInfo taskInfo : taskInfoList) {
        try {
          wireTaskInfoList.add(new alluxio.job.wire.TaskInfo(taskInfo));
        } catch (IOException e) {
          LOG.error("task info deserialization failed " + e);
        }
      }
      return new JobHeartbeatTResponse(mJobMaster.workerHeartbeat(workerId, wireTaskInfoList));
    }, "Heartbeat", "workerId=%s, taskInfoList=%s, options=%s", workerId, taskInfoList, options);
  }
}
