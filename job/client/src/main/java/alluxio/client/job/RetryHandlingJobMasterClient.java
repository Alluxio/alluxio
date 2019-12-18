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

package alluxio.client.job;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.grpc.CancelPRequest;
import alluxio.grpc.GetAllWorkerHealthPRequest;
import alluxio.grpc.GetJobServiceSummaryPRequest;
import alluxio.grpc.GetJobStatusPRequest;
import alluxio.grpc.JobMasterClientServiceGrpc;
import alluxio.grpc.ListAllPRequest;
import alluxio.grpc.RunPRequest;
import alluxio.grpc.ServiceType;
import alluxio.job.JobConfig;
import alluxio.job.ProtoUtils;
import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.worker.job.JobMasterClientContext;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the job service master, used by job service
 * clients.
 */
@ThreadSafe
public final class RetryHandlingJobMasterClient extends AbstractMasterClient
    implements JobMasterClient {
  private JobMasterClientServiceGrpc.JobMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new job master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingJobMasterClient(JobMasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.JOB_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.JOB_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.JOB_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = JobMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public void cancel(final long jobId) throws IOException {
    retryRPC((RpcCallable<Void>) () -> {
      mClient.cancel(CancelPRequest.newBuilder().setJobId(jobId).build());
      return null;
    });
  }

  @Override
  public JobInfo getJobStatus(final long id) throws IOException {
    return ProtoUtils.fromProto(retryRPC(new RpcCallable<alluxio.grpc.JobInfo>() {
      public alluxio.grpc.JobInfo call() throws StatusRuntimeException {
        return mClient.getJobStatus(GetJobStatusPRequest.newBuilder().setJobId(id).build())
            .getJobInfo();
      }
    }));
  }

  @Override
  public JobServiceSummary getJobServiceSummary() throws IOException {
    return new JobServiceSummary(retryRPC((RpcCallable<alluxio.grpc.JobServiceSummary>) () -> {
      return mClient.getJobServiceSummary(
              GetJobServiceSummaryPRequest.newBuilder().build()).getSummary();
    }));
  }

  @Override
  public List<Long> list() throws IOException {
    return retryRPC(new RpcCallable<List<Long>>() {
      public List<Long> call() {
        return mClient.listAll(ListAllPRequest.getDefaultInstance()).getJobIdsList();
      }
    });
  }

  @Override
  public List<JobInfo> listDetailed() throws IOException {
    List<alluxio.grpc.JobInfo> jobProtoInfos = retryRPC(
        new RpcCallable<List<alluxio.grpc.JobInfo>>() {
          public List<alluxio.grpc.JobInfo> call() {
            return mClient.listAll(ListAllPRequest.getDefaultInstance())
                .getJobInfosList();
          }
        });
    ArrayList<JobInfo> result = Lists.newArrayList();
    for (alluxio.grpc.JobInfo jobProtoInfo : jobProtoInfos) {
      result.add(ProtoUtils.fromProto(jobProtoInfo));
    }
    return result;
  }

  @Override
  public long run(final JobConfig jobConfig) throws IOException {
    final ByteString jobConfigStr = ByteString.copyFrom(SerializationUtils.serialize(jobConfig));
    return retryRPC(new RpcCallable<Long>() {
      public Long call() throws StatusRuntimeException {
        return mClient.run(RunPRequest.newBuilder().setJobConfig(jobConfigStr).build()).getJobId();
      }
    });
  }

  @Override
  public List<JobWorkerHealth> getAllWorkerHealth() throws IOException {
    return retryRPC(new RpcCallable<List<JobWorkerHealth>>() {
      public List<JobWorkerHealth> call() throws StatusRuntimeException {
        List<alluxio.grpc.JobWorkerHealth> workerHealthsList = mClient
            .getAllWorkerHealth(GetAllWorkerHealthPRequest.newBuilder().build())
            .getWorkerHealthsList();

        return workerHealthsList.stream().map(JobWorkerHealth::new).collect(Collectors.toList());
      }
    });
  }
}
