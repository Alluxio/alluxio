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

import alluxio.grpc.CopyJobPOptions;
import alluxio.job.CopyJobRequest;
import alluxio.master.file.FileSystemMaster;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Factory for creating {@link LoadJob}s that get file infos from master.
 */
public class CopyJobFactory implements JobFactory {

  private final FileSystemMaster mFsMaster;
  private final CopyJobRequest mRequest;

  /**
   * Create factory.
   * @param request load job request
   * @param fsMaster file system master
   */
  public CopyJobFactory(CopyJobRequest request, FileSystemMaster fsMaster) {
    mFsMaster = fsMaster;
    mRequest = request;
  }

  @Override
  public Job<?> create() {
    CopyJobPOptions options = mRequest.getOptions();
    String src = mRequest.getSrc();
    OptionalLong bandwidth =
        options.hasBandwidth() ? OptionalLong.of(options.getBandwidth()) : OptionalLong.empty();
    boolean partialListing = options.hasPartialListing() && options.getPartialListing();
    boolean verificationEnabled = options.hasVerify() && options.getVerify();
    FileIterable fileIterator = new FileIterable(mFsMaster, src, Optional
        .ofNullable(AuthenticatedClientUser.getOrNull())
        .map(User::getName), partialListing,
        LoadJob.QUALIFIED_FILE_FILTER);
    Optional<String> user = Optional
        .ofNullable(AuthenticatedClientUser.getOrNull())
        .map(User::getName);
    return new CopyJob(src, mRequest.getDst(), user, UUID.randomUUID().toString(),
        bandwidth,
        partialListing,
        verificationEnabled, fileIterator);
  }
}

