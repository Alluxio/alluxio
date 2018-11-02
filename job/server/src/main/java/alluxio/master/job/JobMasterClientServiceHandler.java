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
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.JobConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CancelTOptions;
import alluxio.thrift.CancelTResponse;
import alluxio.thrift.GetJobStatusTOptions;
import alluxio.thrift.GetJobStatusTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.JobMasterClientService;
import alluxio.thrift.ListAllTOptions;
import alluxio.thrift.ListAllTResponse;
import alluxio.thrift.RunTOptions;
import alluxio.thrift.RunTResponse;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * This class is a Thrift handler for job master RPCs invoked by a job service client.
 */
public class JobMasterClientServiceHandler implements JobMasterClientService.Iface {
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
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.JOB_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public CancelTResponse cancel(final long id, CancelTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcUtils.RpcCallable<CancelTResponse>) () -> {
      mJobMaster.cancel(id);
      return new CancelTResponse();
    }, "Cancel", "id=%s, options=%s", id, options);
  }

  @Override
  public GetJobStatusTResponse getJobStatus(final long id, GetJobStatusTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetJobStatusTResponse>) () ->
        new GetJobStatusTResponse(mJobMaster.getStatus(id).toThrift()),
        "GetJobStatus", "id=%s, options=%s", id, options
    );
  }

  @Override
  public ListAllTResponse listAll(ListAllTOptions options) throws AlluxioTException {
    return RpcUtils.call(
        LOG, (RpcUtils.RpcCallable<ListAllTResponse>) () -> new ListAllTResponse(mJobMaster.list()),
        "ListAll", "options=%s", options);
  }

  @Override
  public RunTResponse run(final ByteBuffer jobConfig, RunTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<RunTResponse>) () -> {
      try {
        byte[] jobConfigBytes = new byte[jobConfig.remaining()];
        jobConfig.get(jobConfigBytes);
        return new RunTResponse(
            mJobMaster.run((JobConfig) SerializationUtils.deserialize(jobConfigBytes)));
      } catch (ClassNotFoundException e) {
        throw new InvalidArgumentException(e);
      }
    }, "Run", "jobConfig=%s, options=%s", jobConfig, options);
  }
}
