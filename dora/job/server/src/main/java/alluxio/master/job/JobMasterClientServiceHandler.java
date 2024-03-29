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
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.CancelPRequest;
import alluxio.grpc.CancelPResponse;
import alluxio.grpc.GetAllMasterStatusPRequest;
import alluxio.grpc.GetAllMasterStatusPResponse;
import alluxio.grpc.GetAllWorkerHealthPRequest;
import alluxio.grpc.GetAllWorkerHealthPResponse;
import alluxio.grpc.GetCmdStatusDetailedRequest;
import alluxio.grpc.GetCmdStatusDetailedResponse;
import alluxio.grpc.GetCmdStatusRequest;
import alluxio.grpc.GetCmdStatusResponse;
import alluxio.grpc.GetJobServiceSummaryPRequest;
import alluxio.grpc.GetJobServiceSummaryPResponse;
import alluxio.grpc.GetJobStatusDetailedPRequest;
import alluxio.grpc.GetJobStatusDetailedPResponse;
import alluxio.grpc.GetJobStatusPRequest;
import alluxio.grpc.GetJobStatusPResponse;
import alluxio.grpc.JobMasterClientServiceGrpc;
import alluxio.grpc.JobMasterStatus;
import alluxio.grpc.ListAllPRequest;
import alluxio.grpc.ListAllPResponse;
import alluxio.grpc.RunPRequest;
import alluxio.grpc.RunPResponse;
import alluxio.grpc.SubmitRequest;
import alluxio.grpc.SubmitResponse;
import alluxio.job.CmdConfig;
import alluxio.job.JobConfig;
import alluxio.job.util.SerializationUtils;
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
  private final JobMaster mJobMaster;

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
    RpcUtils.call(LOG, () -> {
      mJobMaster.cancel(request.getJobId());
      return CancelPResponse.getDefaultInstance();
    }, "cancel", "request=%s", responseObserver, request);
  }

  @Override
  public void getJobStatus(GetJobStatusPRequest request,
      StreamObserver<GetJobStatusPResponse> responseObserver) {
    RpcUtils.call(LOG,
        () -> GetJobStatusPResponse.newBuilder()
            .setJobInfo(mJobMaster.getStatus(request.getJobId(), false).toProto()).build(),
        "getJobStatus", "request=%s", responseObserver, request);
  }

  @Override
  public void getJobStatusDetailed(GetJobStatusDetailedPRequest request,
      StreamObserver<GetJobStatusDetailedPResponse> responseObserver) {
    RpcUtils.call(LOG,
        () -> GetJobStatusDetailedPResponse.newBuilder()
            .setJobInfo(mJobMaster.getStatus(request.getJobId(), true).toProto()).build(),
        "getJobStatusDetailed", "request=%s", responseObserver, request);
  }

  @Override
  public void getJobServiceSummary(GetJobServiceSummaryPRequest request,
      StreamObserver<GetJobServiceSummaryPResponse> responseObserver) {
    RpcUtils.call(LOG,
        () -> GetJobServiceSummaryPResponse.newBuilder()
            .setSummary(mJobMaster.getSummary().toProto()).build(),
        "getJobServiceSummary", "request=%s", responseObserver, request);
  }

  @Override
  public void listAll(ListAllPRequest request, StreamObserver<ListAllPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      List<Long> jobList = mJobMaster.list(request.getOptions());
      ListAllPResponse.Builder builder = ListAllPResponse.newBuilder().addAllJobIds(jobList);
      if (!(request.getOptions().hasJobIdOnly() && request.getOptions().getJobIdOnly())) {
        for (Long id : jobList) {
          builder.addJobInfos(mJobMaster.getStatus(id).toProto());
        }
      }
      return builder.build();
    }, "listAll", "request=%s", responseObserver, request);
  }

  @Override
  public void run(RunPRequest request, StreamObserver<RunPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
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
    RpcUtils.call(LOG, () -> {
      GetAllWorkerHealthPResponse.Builder builder = GetAllWorkerHealthPResponse.newBuilder();

      List<JobWorkerHealth> workerHealths = mJobMaster.getAllWorkerHealth();

      for (JobWorkerHealth workerHealth : workerHealths) {
        builder.addWorkerHealths(workerHealth.toProto());
      }

      return builder.build();
    }, "getAllWorkerHealth", "request=%s", responseObserver, request);
  }

  @Override
  public void getAllMasterStatus(GetAllMasterStatusPRequest request,
                                 StreamObserver<GetAllMasterStatusPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      GetAllMasterStatusPResponse.Builder builder = GetAllMasterStatusPResponse.newBuilder();

      List<JobMasterStatus> masterStatuses = mJobMaster.getAllJobMasterStatus();

      for (JobMasterStatus masterStatus : masterStatuses) {
        builder.addJobMasterStatus(masterStatus);
      }

      return builder.build();
    }, "getAllMasterStatus", "request=%s", responseObserver, request);
  }

  @Override
  public void submit(SubmitRequest request, StreamObserver<SubmitResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      try {
        byte[] cmdConfigBytes = request.getCmdConfig().toByteArray();
        return SubmitResponse.newBuilder()
            .setJobControlId(
                mJobMaster.submit((CmdConfig) SerializationUtils.deserialize(cmdConfigBytes)))
            .build();
      } catch (ClassNotFoundException e) {
        throw new InvalidArgumentException(e);
      }
    }, "Submit", "request=%s", responseObserver, request);
  }

  @Override
  public void getCmdStatus(GetCmdStatusRequest request,
      StreamObserver<GetCmdStatusResponse> responseObserver) {
    RpcUtils.call(LOG,
        () -> GetCmdStatusResponse.newBuilder()
            .setCmdStatus(mJobMaster.getCmdStatus(request.getJobControlId()).toProto()).build(),
        "GetCmdStatus", "request=%s", responseObserver, request);
  }

  @Override
  public void getCmdStatusDetailed(GetCmdStatusDetailedRequest request,
      StreamObserver<GetCmdStatusDetailedResponse> responseObserver) {
    RpcUtils
        .call(LOG,
            () -> GetCmdStatusDetailedResponse.newBuilder()
                .setCmdStatusBlock(
                    mJobMaster.getCmdStatusDetailed(request.getJobControlId()).toProto())
                .build(),
            "getCmdStatusDetailed", "request=%s", responseObserver, request);
  }
}
