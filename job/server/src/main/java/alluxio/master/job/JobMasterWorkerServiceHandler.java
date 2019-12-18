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

import alluxio.RpcUtils;
import alluxio.grpc.JobHeartbeatPRequest;
import alluxio.grpc.JobHeartbeatPResponse;
import alluxio.grpc.JobMasterWorkerServiceGrpc;
import alluxio.grpc.RegisterJobWorkerPRequest;
import alluxio.grpc.RegisterJobWorkerPResponse;
import alluxio.grpc.GrpcUtils;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.TaskInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a gRPC handler for job master RPCs invoked by a job service worker.
 */
@ThreadSafe
public final class JobMasterWorkerServiceHandler
    extends JobMasterWorkerServiceGrpc.JobMasterWorkerServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(JobMasterWorkerServiceHandler.class);
  private final JobMaster mJobMaster;

  /**
   * Creates a new instance of {@link JobMasterWorkerServiceHandler}.
   *
   * @param JobMaster the {@link JobMaster} that the handler uses internally
   */
  public JobMasterWorkerServiceHandler(JobMaster JobMaster) {
    mJobMaster = Preconditions.checkNotNull(JobMaster);
  }

  @Override
  public void heartbeat(JobHeartbeatPRequest request,
                        StreamObserver<JobHeartbeatPResponse> responseObserver) {

    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<JobHeartbeatPResponse>) () -> {
      List<TaskInfo> wireTaskInfoList = Lists.newArrayList();
      for (alluxio.grpc.JobInfo taskInfo : request.getTaskInfosList()) {
        try {
          wireTaskInfoList.add(new TaskInfo(taskInfo));
        } catch (IOException e) {
          LOG.error("task info deserialization failed " + e);
        }
      }
      JobWorkerHealth jobWorkerHealth = new JobWorkerHealth(request.getJobWorkerHealth());
      return JobHeartbeatPResponse.newBuilder()
              .addAllCommands(mJobMaster.workerHeartbeat(jobWorkerHealth, wireTaskInfoList))
              .build();
    }, "heartbeat", "request=%s", responseObserver, request);
  }

  @Override
  public void registerJobWorker(RegisterJobWorkerPRequest request,
      StreamObserver<RegisterJobWorkerPResponse> responseObserver) {

    RpcUtils.call(LOG,
        (RpcUtils.RpcCallableThrowsIOException<RegisterJobWorkerPResponse>) () -> {
          return RegisterJobWorkerPResponse.newBuilder()
              .setId(mJobMaster.registerWorker(GrpcUtils.fromProto(request.getWorkerNetAddress())))
              .build();
        }, "registerJobWorker", "request=%s", responseObserver, request);
  }
}
