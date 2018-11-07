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
import alluxio.job.JobConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.JobInfo;
import alluxio.thrift.AlluxioService.Client;
import alluxio.thrift.CancelTOptions;
import alluxio.thrift.GetJobStatusTOptions;
import alluxio.thrift.JobMasterClientService;
import alluxio.thrift.ListAllTOptions;
import alluxio.thrift.RunTOptions;
import alluxio.worker.job.JobMasterClientConfig;

import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the job service master, used by job service
 * clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class RetryHandlingJobMasterClient extends AbstractMasterClient
    implements JobMasterClient {
  private JobMasterClientService.Client mClient = null;

  /**
   * Creates a new job master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingJobMasterClient(JobMasterClientConfig conf) {
    super(conf);
  }

  @Override
  protected Client getClient() {
    return mClient;
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
  protected void beforeConnect() throws IOException {
    // Job master client does not load cluster-default configuration because only the master
    // will use this client
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new JobMasterClientService.Client(mProtocol);
  }

  @Override
  public synchronized void cancel(final long jobId) throws IOException {
    retryRPC((RpcCallable<Void>) () -> {
      mClient.cancel(jobId, new CancelTOptions());
      return null;
    });
  }

  @Override
  public synchronized JobInfo getStatus(final long jobId) throws IOException {
    return new JobInfo(retryRPC(new RpcCallable<alluxio.thrift.JobInfo>() {
      public alluxio.thrift.JobInfo call() throws TException {
        return mClient.getJobStatus(jobId, new GetJobStatusTOptions()).getJobInfo();
      }
    }));
  }

  @Override
  public synchronized List<Long> list() throws IOException {
    return retryRPC(new RpcCallable<List<Long>>() {
      public List<Long> call() throws TException {
        return mClient.listAll(new ListAllTOptions()).getJobIdList();
      }
    });
  }

  @Override
  public synchronized long run(final JobConfig jobConfig) throws IOException {
    final ByteBuffer configBytes = ByteBuffer.wrap(SerializationUtils.serialize(jobConfig));
    return retryRPC(new RpcCallable<Long>() {
      public Long call() throws TException {
        return mClient.run(configBytes, new RunTOptions()).getJobId();
      }
    });
  }
}
