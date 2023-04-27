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
import alluxio.client.WriteType;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CopyJobPOptions;
import alluxio.job.CopyJobRequest;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UnderFileSystem;
import alluxio.wire.FileInfo;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Factory for creating {@link LoadJob}s that get file infos from master.
 */
public class CopyJobFactory implements JobFactory {

  private final UnderFileSystem mFs;
  private final CopyJobRequest mRequest;

  /**
   * Create factory.
   * @param request load job request
   * @param underFileSystem file system master
   */
  public CopyJobFactory(CopyJobRequest request, UnderFileSystem underFileSystem) {
    mFs = underFileSystem;
    mRequest = request;
  }

  @Override
  public Job<?> create() {
    CopyJobPOptions options = mRequest.getOptions();
    String src = mRequest.getSrc();
    String srcRoot = new AlluxioURI(src).getRootPath();
    String dstRoot = new AlluxioURI(mRequest.getDst()).getRootPath();
    OptionalLong bandwidth =
        options.hasBandwidth() ? OptionalLong.of(options.getBandwidth()) : OptionalLong.empty();
    boolean partialListing = options.hasPartialListing() && options.getPartialListing();
    boolean verificationEnabled = options.hasVerify() && options.getVerify();
    boolean overwrite = options.hasOverwrite() && options.getOverwrite();
    WriteType writeType = options.hasWriteType() ? WriteType.fromProto(options.getWriteType()) :
        Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    Iterable<FileInfo> fileIterator = new UfsFileIterable(mFs, src, Optional
        .ofNullable(AuthenticatedClientUser.getOrNull())
        .map(User::getName), partialListing, FileInfo::isCompleted, srcRoot);
    Optional<String> user = Optional
        .ofNullable(AuthenticatedClientUser.getOrNull())
        .map(User::getName);
    return new CopyJob(src, mRequest.getDst(), srcRoot,
        dstRoot, overwrite, writeType, user, UUID.randomUUID().toString(),
        bandwidth, partialListing, verificationEnabled, fileIterator);
  }
}

