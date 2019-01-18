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

package alluxio.job.replicate;

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.ServerConfiguration;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job to replicate a block. This job is invoked by the checker of replication level in
 * FileSystemMaster.
 */
@NotThreadSafe
public final class ReplicateDefinition
    extends AbstractVoidJobDefinition<ReplicateConfig, SerializableVoid> {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicateDefinition.class);

  private final FileSystem mFileSystem;
  private final FileSystemContext mFsContext;

  /**
   * Constructs a new {@link ReplicateDefinition} instance with FileSystem context and instance.
   *
   */
  public ReplicateDefinition() {
    mFsContext = FileSystemContext.create(ServerConfiguration.global());
    mFileSystem = BaseFileSystem.create(mFsContext);
  }

  /**
   * Constructs a new {@link ReplicateDefinition} instance.
   *
   * @param fsContext the {@link FileSystemContext} used by the {@link FileSystem}
   * @param fileSystem the {@link FileSystem} client
   */
  public ReplicateDefinition(FileSystemContext fsContext, FileSystem fileSystem) {
    mFsContext = fsContext;
    mFileSystem = fileSystem;
  }

  @Override
  public Class<ReplicateConfig> getJobConfigClass() {
    return ReplicateConfig.class;
  }

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(ReplicateConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Preconditions.checkArgument(!jobWorkerInfoList.isEmpty(), "No worker is available");

    long blockId = config.getBlockId();
    int numReplicas = config.getReplicas();
    Preconditions.checkArgument(numReplicas > 0);

    AlluxioBlockStore blockStore = AlluxioBlockStore.create(mFsContext);
    BlockInfo blockInfo = blockStore.getInfo(blockId);

    Set<String> hosts = new HashSet<>();
    for (BlockLocation blockLocation : blockInfo.getLocations()) {
      hosts.add(blockLocation.getWorkerAddress().getHost());
    }
    Map<WorkerInfo, SerializableVoid> result = Maps.newHashMap();

    Collections.shuffle(jobWorkerInfoList);
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      // Select job workers that don't have this block locally to replicate
      if (!hosts.contains(workerInfo.getAddress().getHost())) {
        result.put(workerInfo, null);
        if (result.size() >= numReplicas) {
          break;
        }
      }
    }
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * This task will replicate the block.
   */
  @Override
  public SerializableVoid runTask(ReplicateConfig config, SerializableVoid arg,
      JobWorkerContext jobWorkerContext) throws Exception {
    JobUtils.loadBlock(mFileSystem, mFsContext, config.getPath(), config.getBlockId());
    return null;
  }
}
