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

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.grpc.LoadJobPOptions;
import alluxio.job.LoadJobRequest;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.predicate.FilePredicate;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.base.Predicates;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * Factory for creating {@link LoadJob}s that get file infos from master.
 */
public class LoadJobFactory implements JobFactory {

  private final LoadJobRequest mRequest;
  private final DefaultFileSystemMaster mFs;

  /**
   * Create factory.
   * @param request load job request
   * @param fsMaster file system master
   */
  public LoadJobFactory(LoadJobRequest request, DefaultFileSystemMaster fsMaster) {
    mRequest = request;
    mFs = fsMaster;
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
    int replicas = options.hasReplicas() ? options.getReplicas() : 1;

    Predicate<UfsStatus> predicate = Predicates.alwaysTrue();
    Optional<String> fileFilterRegx = Optional.empty();
    if (options.hasFileFilterRegx()) {
      String regxPatternStr = options.getFileFilterRegx();
      if (regxPatternStr != null && !regxPatternStr.isEmpty()) {
        alluxio.proto.journal.Job.FileFilter.Builder builder =
            alluxio.proto.journal.Job.FileFilter.newBuilder()
                .setName("fileNamePattern").setValue(regxPatternStr);
        FilePredicate filePredicate = FilePredicate.create(builder.build());
        predicate = filePredicate.getUfsStatusPredicate();
        fileFilterRegx = Optional.of(regxPatternStr);
      }
    }

    UnderFileSystem ufs = mFs.getUfsManager().getOrAdd(new AlluxioURI(path),
        () -> UnderFileSystemConfiguration.defaults(Configuration.global()));
    Iterable<UfsStatus> iterable = new UfsStatusIterable(ufs, path,
        Optional.ofNullable(AuthenticatedClientUser.getOrNull()).map(User::getName),
        predicate);
    return new DoraLoadJob(path, user, UUID.randomUUID().toString(), bandwidth, partialListing,
        verificationEnabled, options.getLoadMetadataOnly(), options.getSkipIfExists(),
        fileFilterRegx, iterable.iterator(), ufs, 1);
  }
}

