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

package alluxio.job.plan.migrate;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.job.plan.AbstractVoidPlanDefinition;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.util.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.util.io.PathUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * A job that migrates a single file from source path to a destination path.
 * The source and the destination can be across mount points.
 */
public final class MigrateDefinition
    extends AbstractVoidPlanDefinition<MigrateConfig, MigrateCommand> {
  private static final Logger LOG = LoggerFactory.getLogger(MigrateDefinition.class);

  private static final int JOBS_PER_WORKER = 10;

  private final Random mRandom = new Random();

  /**
   * Constructs a new {@link MigrateDefinition}.
   */
  public MigrateDefinition() {
  }

  private void checkMigrateValid(MigrateConfig config, FileSystem fs) throws Exception {
    AlluxioURI source = new AlluxioURI(config.getSource());
    AlluxioURI destination = new AlluxioURI(config.getDestination());
    // The source cannot be a prefix of the destination -
    // that would be migrating a path inside itself.
    if (PathUtils.hasPrefix(destination.toString(), source.toString())) {
      throw new RuntimeException(ExceptionMessage.MIGRATE_CANNOT_BE_TO_SUBDIRECTORY.getMessage(
          source, config.getDestination()));
    }
  }

  /**
   * {@inheritDoc}
   *
   * Assigns each worker to migrate whichever files it has the most blocks for.
   * If no worker has blocks for a file, a random worker is chosen.
   * @return
   */
  @Override
  public Set<Pair<WorkerInfo, MigrateCommand>> selectExecutors(MigrateConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context) throws Exception {
    AlluxioURI source = new AlluxioURI(config.getSource());
    AlluxioURI destination = new AlluxioURI(config.getDestination());
    if (source.equals(destination)) {
      return Sets.newHashSet();
    }
    checkMigrateValid(config, context.getFileSystem());
    Preconditions.checkState(!jobWorkerInfoList.isEmpty(), "No workers are available");

    URIStatus status = context.getFileSystem().getStatus(source);
    ConcurrentMap<String, WorkerInfo> hostnameToWorker = Maps.newConcurrentMap();
    for (WorkerInfo workerInfo : jobWorkerInfoList) {
      hostnameToWorker.put(workerInfo.getAddress().getHost(), workerInfo);
    }
    List<BlockWorkerInfo> alluxioWorkerInfoList = context.getFsContext().getCachedWorkers();
    if (status.isFolder()) {
      throw new RuntimeException(ExceptionMessage.MIGRATE_DIRECTORY.getMessage());
    } else {
      WorkerInfo bestJobWorker =
          getBestJobWorker(status, alluxioWorkerInfoList, jobWorkerInfoList, hostnameToWorker);
      Set<Pair<WorkerInfo, MigrateCommand>> result = Sets.newHashSet();
      result.add(new Pair<>(bestJobWorker,
          new MigrateCommand(status.getPath(), destination.getPath())));
      return result;
    }
  }

  private WorkerInfo getBestJobWorker(URIStatus status, List<BlockWorkerInfo> alluxioWorkerInfoList,
      List<WorkerInfo> jobWorkerInfoList, Map<String, WorkerInfo> hostnameToJobWorker) {
    BlockWorkerInfo bestWorker =
        JobUtils.getWorkerWithMostBlocks(alluxioWorkerInfoList, status.getFileBlockInfos());
    if (bestWorker == null) {
      // Nobody has blocks, choose a random worker.
      bestWorker = alluxioWorkerInfoList.get(mRandom.nextInt(jobWorkerInfoList.size()));
    }
    // Map the best Alluxio worker to a job worker.
    WorkerInfo worker = hostnameToJobWorker.get(bestWorker.getNetAddress().getHost());
    if (worker == null) {
      return jobWorkerInfoList.get(new Random().nextInt(jobWorkerInfoList.size()));
    }
    return worker;
  }

  /**
   * {@inheritDoc}
   *
   * Migrates the file specified in the config to the configured path. If the destination path is a
   * directory, the file is migrated inside that directory.
   */
  @Override
  public SerializableVoid runTask(MigrateConfig config, MigrateCommand command,
      RunTaskContext context) throws Exception {
    WriteType writeType = config.getWriteType() == null
        ? ServerConfiguration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class)
        : WriteType.valueOf(config.getWriteType());
    migrate(command, writeType.toProto(), context.getFileSystem(), config.isOverwrite());
    return null;
  }

  /**
   * @param command the migrate command to execute
   * @param writeType the write type to use for the moved file
   * @param fileSystem the Alluxio file system
   * @param overwrite whether to overwrite destination
   */
  private static void migrate(MigrateCommand command,
                              WritePType writeType,
                              FileSystem fileSystem,
                              boolean overwrite) throws Exception {
    String source = command.getSource();
    String destination = command.getDestination();
    LOG.debug("Migrating {} to {}", source, destination);

    CreateFilePOptions createOptions =
        CreateFilePOptions.newBuilder().setWriteType(writeType).build();
    OpenFilePOptions openFileOptions =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build();
    final AlluxioURI destinationURI = new AlluxioURI(destination);
    boolean retry;
    do {
      retry = false;
      try (FileInStream in = fileSystem.openFile(new AlluxioURI(source), openFileOptions);
           FileOutStream out = fileSystem.createFile(destinationURI, createOptions)) {
        try {
          IOUtils.copyLarge(in, out, new byte[8 * Constants.MB]);
        } catch (Throwable t) {
          try {
            out.cancel();
          } catch (Throwable t2) {
            t.addSuppressed(t2);
          }
          throw t;
        }
      } catch (FileAlreadyExistsException e) {
        if (overwrite) {
          fileSystem.delete(destinationURI);
          retry = true;
        } else {
          throw e;
        }
      }
    } while (retry);
  }

  @Override
  public Class<MigrateConfig> getJobConfigClass() {
    return MigrateConfig.class;
  }
}
