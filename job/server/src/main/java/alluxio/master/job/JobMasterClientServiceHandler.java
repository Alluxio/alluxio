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

import alluxio.ServerRpcUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.CancelPRequest;
import alluxio.grpc.CancelPResponse;
import alluxio.grpc.GetAllWorkerHealthPRequest;
import alluxio.grpc.GetAllWorkerHealthPResponse;
import alluxio.grpc.GetJobServiceSummaryPRequest;
import alluxio.grpc.GetJobServiceSummaryPResponse;
import alluxio.grpc.GetJobStatusDetailedPRequest;
import alluxio.grpc.GetJobStatusDetailedPResponse;
import alluxio.grpc.GetJobStatusPRequest;
import alluxio.grpc.GetJobStatusPResponse;
import alluxio.grpc.JobMasterClientServiceGrpc;
import alluxio.grpc.ListAllPRequest;
import alluxio.grpc.ListAllPResponse;
import alluxio.grpc.RunPRequest;
import alluxio.grpc.RunPResponse;
import alluxio.job.JobConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobWorkerHealth;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This class is a gRPC handler for job master RPCs invoked by a job service client.
 */
public class JobMasterClientServiceHandler
    extends JobMasterClientServiceGrpc.JobMasterClientServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(JobMasterClientServiceHandler.class);
  private static final long RPC_RESPONSE_SIZE_WARNING_THRESHOLD = ServerConfiguration.getBytes(PropertyKey.MASTER_RPC_RESPONSE_SIZE_WARNING_THRESHOLD);
  private JobMaster mJobMaster;

  /**
   * Creates a new instance of {@link JobMasterClientRestServiceHandler}.
   *
   * @param jobMaster the job master to use
   */
  public JobMasterClientServiceHandler(JobMaster jobMaster) {
    Preconditions.checkNotNull(jobMaster);
    mJobMaster = jobMaster;
  }

  @Override
  public void cancel(CancelPRequest request, StreamObserver<CancelPResponse> responseObserver) {
    ServerRpcUtils.call(LOG, (ServerRpcUtils.RpcCallableThrowsIOException<CancelPResponse>) () -> {
      mJobMaster.cancel(request.getJobId());
      return CancelPResponse.getDefaultInstance();
    }, "cancel", "request=%s", responseObserver, request);
  }

  @Override
  public void getJobStatus(GetJobStatusPRequest request,
                           StreamObserver<GetJobStatusPResponse> responseObserver) {
    ServerRpcUtils.call(LOG, (ServerRpcUtils.RpcCallableThrowsIOException<GetJobStatusPResponse>) () -> {
      return GetJobStatusPResponse.newBuilder()
          .setJobInfo(mJobMaster.getStatus(request.getJobId(), false).toProto()).build();
    }, "getJobStatus", "request=%s", responseObserver, request);
  }

  @Override
  public void getJobStatusDetailed(GetJobStatusDetailedPRequest request,
                                   StreamObserver<GetJobStatusDetailedPResponse>
                                       responseObserver) {
    ServerRpcUtils.call(LOG, (ServerRpcUtils.RpcCallableThrowsIOException<GetJobStatusDetailedPResponse>) () ->
    {
      return GetJobStatusDetailedPResponse.newBuilder()
          .setJobInfo(mJobMaster.getStatus(request.getJobId(), true).toProto()).build();
    }, "getJobStatusDetailed", "request=%s", responseObserver, request);
  }

  @Override
  public void getJobServiceSummary(GetJobServiceSummaryPRequest request,
      StreamObserver<GetJobServiceSummaryPResponse> responseObserver) {
    ServerRpcUtils.call(LOG,
        (ServerRpcUtils.RpcCallableThrowsIOException<GetJobServiceSummaryPResponse>) () -> {
          return GetJobServiceSummaryPResponse.newBuilder()
                .setSummary(mJobMaster.getSummary().toProto()).build();
        }, "getJobServiceSummary", "request=%s", responseObserver, request);
  }

  @Override
  public void listAll(ListAllPRequest request, StreamObserver<ListAllPResponse> responseObserver) {
    ServerRpcUtils.call(LOG, (ServerRpcUtils.RpcCallableThrowsIOException<ListAllPResponse>) () -> {
      ListAllPResponse.Builder builder = ListAllPResponse.newBuilder()
          .addAllJobIds(mJobMaster.list());
      for (JobInfo jobInfo : mJobMaster.listDetailed()) {
        builder.addJobInfos(jobInfo.toProto());
      }
      ListAllPResponse response = builder.build();
      if (response.getSerializedSize() > RPC_RESPONSE_SIZE_WARNING_THRESHOLD) {
        LOG.warn("listAll response is {} bytes, {} jobId, {} jobs",
                response.getSerializedSize(),
                response.getJobIdsCount(),
                response.getJobInfosCount());
      }
      return response;
    }, "listAll", "request=%s", responseObserver, request);
  }

  @Override
  public void run(RunPRequest request, StreamObserver<RunPResponse> responseObserver) {
    ServerRpcUtils.call(LOG, (ServerRpcUtils.RpcCallableThrowsIOException<RunPResponse>) () -> {
      try {
        byte[] jobConfigBytes = request.getJobConfig().toByteArray();
        return RunPResponse.newBuilder()
            .setJobId(mJobMaster.run((JobConfig) SerializationUtils.deserialize(jobConfigBytes)))
            .build();
      } catch (ClassNotFoundException e) {
        throw new InvalidArgumentException(e);
      }
    }, "run", "request=%s", responseObserver, request);
  }

  @Override
  public void getAllWorkerHealth(GetAllWorkerHealthPRequest request,
                                 StreamObserver<GetAllWorkerHealthPResponse> responseObserver) {
    ServerRpcUtils.call(LOG, (ServerRpcUtils.RpcCallableThrowsIOException<GetAllWorkerHealthPResponse>) () -> {
      GetAllWorkerHealthPResponse.Builder builder = GetAllWorkerHealthPResponse.newBuilder();

      List<JobWorkerHealth> workerHealths = mJobMaster.getAllWorkerHealth();

      for (JobWorkerHealth workerHealth : workerHealths) {
        builder.addWorkerHealths(workerHealth.toProto());
      }

      GetAllWorkerHealthPResponse response = builder.build();
      if (response.getSerializedSize() > RPC_RESPONSE_SIZE_WARNING_THRESHOLD) {
        LOG.warn("getAllWorkerHealth response is {} bytes, {} workers",
                response.getSerializedSize(),
                workerHealths.size());
      }
      return response;
    }, "getAllWorkerHealth", "request=%s", responseObserver, request);
  }
}
