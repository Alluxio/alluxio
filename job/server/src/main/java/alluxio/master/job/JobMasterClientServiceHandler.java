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

import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.CancelPRequest;
import alluxio.grpc.CancelPResponse;
import alluxio.grpc.GetJobStatusPRequest;
import alluxio.grpc.GetJobStatusPResponse;
import alluxio.grpc.JobMasterClientServiceGrpc;
import alluxio.grpc.ListAllPRequest;
import alluxio.grpc.ListAllPResponse;
import alluxio.grpc.RunPRequest;
import alluxio.grpc.RunPResponse;
import alluxio.job.JobConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.util.RpcUtilsNew;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a gRPC handler for job master RPCs invoked by a job service client.
 */
public class JobMasterClientServiceHandler
    extends JobMasterClientServiceGrpc.JobMasterClientServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(JobMasterClientServiceHandler.class);
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
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<CancelPResponse>) () -> {
      mJobMaster.cancel(request.getJobId());
      return CancelPResponse.getDefaultInstance();
    }, "cancel", "request=%s", responseObserver, request);
  }

  @Override
  public void getJobStatus(GetJobStatusPRequest request,
      StreamObserver<GetJobStatusPResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<GetJobStatusPResponse>) () -> {
      return GetJobStatusPResponse.newBuilder()
          .setJobInfo(mJobMaster.getStatus(request.getJobId()).toProto()).build();
    }, "getJobStatus", "request=%s", responseObserver, request);
  }

  @Override
  public void listAll(ListAllPRequest request, StreamObserver<ListAllPResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<ListAllPResponse>) () -> {
      return ListAllPResponse.newBuilder().addAllJobIds(mJobMaster.list()).build();
    }, "listAll", "request=%s", responseObserver, request);
  }

  @Override
  public void run(RunPRequest request, StreamObserver<RunPResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<RunPResponse>) () -> {
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
}
