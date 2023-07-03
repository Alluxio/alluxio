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
import alluxio.grpc.MoveJobPOptions;
import alluxio.job.MoveJobRequest;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.wire.FileInfo;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Factory for creating {@link MoveJob}s that get file infos from master.
 */
public class MoveJobFactory implements JobFactory {
  private final DefaultFileSystemMaster mFs;
  private final MoveJobRequest mRequest;

  /**
   * Create factory.
   * @param request move job request
   * @param fsMaster file system master
   */
  public MoveJobFactory(MoveJobRequest request, DefaultFileSystemMaster fsMaster) {
    mFs = fsMaster;
    mRequest = request;
  }

  @Override
  public Job<?> create() {
    MoveJobPOptions options = mRequest.getOptions();
    String src = mRequest.getSrc();
    OptionalLong bandwidth =
        options.hasBandwidth() ? OptionalLong.of(options.getBandwidth()) : OptionalLong.empty();
    boolean partialListing = options.hasPartialListing() && options.getPartialListing();
    boolean verificationEnabled = options.hasVerify() && options.getVerify();
    boolean overwrite = options.hasOverwrite() && options.getOverwrite();
    boolean checkContent = options.hasCheckContent() && options.getCheckContent();
    UnderFileSystem ufs = mFs.getUfsManager().getOrAdd(new AlluxioURI(src),
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    Iterable<FileInfo> fileIterator = new UfsFileIterable(ufs, src, Optional
        .ofNullable(AuthenticatedClientUser.getOrNull())
        .map(User::getName), FileInfo::isCompleted);
    Optional<String> user = Optional
        .ofNullable(AuthenticatedClientUser.getOrNull())
        .map(User::getName);
    return new MoveJob(src, mRequest.getDst(), overwrite, user, UUID.randomUUID().toString(),
        bandwidth, partialListing, verificationEnabled, checkContent, fileIterator,
        Optional.empty());
  }
}
