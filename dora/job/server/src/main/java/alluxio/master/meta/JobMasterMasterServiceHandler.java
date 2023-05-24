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

package alluxio.master.meta;

import alluxio.RpcUtils;
import alluxio.grpc.GetJobMasterIdPRequest;
import alluxio.grpc.GetJobMasterIdPResponse;
import alluxio.grpc.JobMasterHeartbeatPRequest;
import alluxio.grpc.JobMasterHeartbeatPResponse;
import alluxio.grpc.JobMasterMasterServiceGrpc;
import alluxio.grpc.NetAddress;
import alluxio.grpc.RegisterJobMasterPRequest;
import alluxio.grpc.RegisterJobMasterPResponse;
import alluxio.master.job.JobMaster;
import alluxio.wire.Address;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a gRPC handler for the primary job master to answer RPC from standby job masters.
 */
@NotThreadSafe
public final class JobMasterMasterServiceHandler
        extends JobMasterMasterServiceGrpc.JobMasterMasterServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(JobMasterMasterServiceHandler.class);

  private final JobMaster mJobMaster;

  /**
   * Creates a new instance of {@link JobMasterMasterServiceHandler}.
   *
   * @param jobMaster the job master from the primary job master process
   */
  public JobMasterMasterServiceHandler(JobMaster jobMaster) {
    LOG.info("Started to serve standby job master requests");
    mJobMaster = jobMaster;
  }

  @Override
  public void getMasterId(GetJobMasterIdPRequest request,
                          StreamObserver<GetJobMasterIdPResponse> responseObserver) {
    NetAddress masterAddress = request.getMasterAddress();
    RpcUtils.call(LOG, () -> GetJobMasterIdPResponse.newBuilder()
                    .setMasterId(mJobMaster.getMasterId(Address.fromProto(masterAddress))).build(),
            "GetJobMasterId", "request=%s", responseObserver, request);
  }

  @Override
  public void registerMaster(RegisterJobMasterPRequest request,
                             StreamObserver<RegisterJobMasterPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mJobMaster.jobMasterRegister(request.getJobMasterId(), request.getOptions());
      return RegisterJobMasterPResponse.getDefaultInstance();
    }, "RegisterJobMaster", "request=%s", responseObserver, request);
  }

  @Override
  public void masterHeartbeat(JobMasterHeartbeatPRequest request,
                              StreamObserver<JobMasterHeartbeatPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> JobMasterHeartbeatPResponse.newBuilder().setCommand(
        mJobMaster.jobMasterHeartbeat(request.getMasterId(), request.getOptions())).build(),
        "JobMasterHeartbeat", "request=%s", responseObserver, request);
  }
}
