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

import alluxio.grpc.LoadJobPOptions;
import alluxio.job.LoadJobRequest;
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
public class LoadJobFactory implements JobFactory {

  private final LoadJobRequest mRequest;

  /**
   * Create factory.
   * @param request load job request
   */
  public LoadJobFactory(LoadJobRequest request) {
    mRequest = request;
  }

  @Override
  public Job<?> create() {
    LoadJobPOptions options = mRequest.getOptions();
    String path = mRequest.getPath();
    OptionalLong bandwidth =
        options.hasBandwidth() ? OptionalLong.of(options.getBandwidth()) : OptionalLong.empty();
    boolean partialListing = options.hasPartialListing() && options.getPartialListing();
    boolean verificationEnabled = options.hasVerify() && options.getVerify();
    Optional<String> user = Optional
        .ofNullable(AuthenticatedClientUser.getOrNull())
        .map(User::getName);
    return new DoraLoadJob(path, user, UUID.randomUUID().toString(),
        bandwidth,
        partialListing,
        verificationEnabled,
        options.getLoadMetadataOnly()
    );
  }
}

