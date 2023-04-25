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

import alluxio.AbstractJobMasterClient;
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.BuildVersion;
import alluxio.grpc.GetJobMasterIdPRequest;
import alluxio.grpc.JobMasterHeartbeatPOptions;
import alluxio.grpc.JobMasterHeartbeatPRequest;
import alluxio.grpc.JobMasterMasterServiceGrpc;
import alluxio.grpc.JobMasterMetaCommand;
import alluxio.grpc.RegisterJobMasterPOptions;
import alluxio.grpc.RegisterJobMasterPRequest;
import alluxio.grpc.ServiceType;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.wire.Address;
import alluxio.worker.job.JobMasterClientContext;

import com.codahale.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the primary job master,
 * used by Alluxio standby job masters.
 */
@ThreadSafe
public final class RetryHandlingJobMasterMasterClient extends AbstractJobMasterClient {
  private static final Logger LOG =
          LoggerFactory.getLogger(RetryHandlingJobMasterMasterClient.class);
  private JobMasterMasterServiceGrpc.JobMasterMasterServiceBlockingStub mClient = null;

  /**
   * Creates a instance of {@link RetryHandlingJobMasterMasterClient}.
   *
   * @param conf master client configuration
   */
  public RetryHandlingJobMasterMasterClient(JobMasterClientContext conf) {
    super(conf);
  }

  @Override
  public void connect() throws AlluxioStatusException {
    super.connect();
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.JOB_MASTER_MASTER_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.JOB_MASTER_MASTER_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.JOB_MASTER_MASTER_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = JobMasterMasterServiceGrpc.newBlockingStub(mChannel);
  }

  /**
   * Returns a master id for a master address.
   *
   * @param address the address to get a master id for
   * @return a master id
   */
  public long getId(final Address address) throws IOException {
    return retryRPC(() -> mClient
        .getMasterId(GetJobMasterIdPRequest.newBuilder()
            .setMasterAddress(address.toProto()).build())
        .getMasterId(), LOG, "GetId", "address=%s", address);
  }

  /**
   * Sends a heartbeat to the leader master. Standby masters periodically execute this method
   * so that the leader master knows they are still running.
   *
   * @param masterId the master id
   * @return whether this master should re-register
   */
  public JobMasterMetaCommand heartbeat(final long masterId) throws IOException {
    return retryRPC(() -> mClient
        .masterHeartbeat(JobMasterHeartbeatPRequest.newBuilder().setMasterId(masterId)
            .setOptions(JobMasterHeartbeatPOptions.getDefaultInstance()).build())
        .getCommand(), LOG, "JobMasterHeartbeat", "masterId=%d", masterId);
  }

  /**
   * Registers with the leader master.
   *
   * @param masterId the master id of the standby master registering
   */
  public void register(final long masterId)
          throws IOException {
    final Map<String, Gauge> gauges = MetricsSystem.METRIC_REGISTRY.getGauges();
    RegisterJobMasterPOptions.Builder optionsBuilder = RegisterJobMasterPOptions.newBuilder()
        .setVersion(BuildVersion.newBuilder().setVersion(RuntimeConstants.VERSION)
            .setRevision(RuntimeConstants.REVISION_SHORT).build());
    Gauge startTimeGauge = gauges.get(MetricKey.MASTER_START_TIME.getName());
    if (startTimeGauge != null) {
      optionsBuilder.setStartTimeMs((long) startTimeGauge.getValue());
    }
    Gauge lastLosePrimacyGuage = gauges.get(MetricKey.MASTER_LAST_LOSE_PRIMACY_TIME.getName());
    if (lastLosePrimacyGuage != null) {
      optionsBuilder.setLosePrimacyTimeMs((long) lastLosePrimacyGuage.getValue());
    }
    retryRPC(() -> {
      mClient.registerMaster(RegisterJobMasterPRequest.newBuilder().setJobMasterId(masterId)
              .setOptions(optionsBuilder).build());
      return null;
    }, LOG, "Register", "jobMasterId=%d", masterId);
  }
}
