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

package alluxio.job.persist;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.collections.Pair;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job that persists a file into the under storage.
 */
@NotThreadSafe
public final class PersistDefinition
    extends AbstractVoidJobDefinition<PersistConfig, SerializableVoid> {
  private static final Logger LOG = LoggerFactory.getLogger(PersistDefinition.class);
  private final FileSystemContext mFileSystemContext;
  private final FileSystem mFileSystem;

  /**
   * Constructs a new {@link PersistDefinition}.
   */
  public PersistDefinition() {
    mFileSystemContext = FileSystemContext.get();
    mFileSystem = BaseFileSystem.get(FileSystemContext.get());
  }

  /**
   * Constructs a new {@link PersistDefinition} with FileSystem context and instance.
   *
   * @param context file system context
   * @param fileSystem file system client
   */
  public PersistDefinition(FileSystemContext context, FileSystem fileSystem) {
    mFileSystemContext = context;
    mFileSystem = fileSystem;
  }

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(PersistConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    if (jobWorkerInfoList.isEmpty()) {
      throw new RuntimeException("No worker is available");
    }

    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    List<BlockWorkerInfo> alluxioWorkerInfoList =
        AlluxioBlockStore.create(mFileSystemContext).getAllWorkers();
    BlockWorkerInfo workerWithMostBlocks = JobUtils.getWorkerWithMostBlocks(alluxioWorkerInfoList,
        mFileSystem.getStatus(uri).getFileBlockInfos());

    // Map the best Alluxio worker to a job worker.
    Map<WorkerInfo, SerializableVoid> result = Maps.newHashMap();
    boolean found = false;
    if (workerWithMostBlocks != null) {
      for (WorkerInfo workerInfo : jobWorkerInfoList) {
        if (workerInfo.getAddress().getHost()
            .equals(workerWithMostBlocks.getNetAddress().getHost())) {
          result.put(workerInfo, null);
          found = true;
          break;
        }
      }
    }
    if (!found) {
      result.put(jobWorkerInfoList.get(new Random().nextInt(jobWorkerInfoList.size())), null);
    }

    return result;
  }

  @Override
  public SerializableVoid runTask(PersistConfig config, SerializableVoid args,
      JobWorkerContext context) throws Exception {
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    String ufsPath = config.getUfsPath();

    // check if the file is persisted in UFS and delete it, if we are overwriting it
    UfsManager.UfsClient ufsClient = context.getUfsManager().get(config.getMountId());
    try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      if (ufs == null) {
        throw new IOException("Failed to create UFS instance for " + ufsPath);
      }
      if (ufs.exists(ufsPath)) {
        if (config.isOverwrite()) {
          LOG.info("File {} is already persisted in UFS. Removing it.", config.getFilePath());
          ufs.deleteFile(ufsPath);
        } else {
          throw new IOException("File " + config.getFilePath()
              + " is already persisted in UFS, to overwrite the file, please set the overwrite flag"
              + " in the config.");
        }
      }

      FileSystem fs = FileSystem.Factory.get();
      long bytesWritten;
      try (Closer closer = Closer.create()) {
        OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
        FileInStream in = closer.register(fs.openFile(uri, options));
        AlluxioURI dstPath = new AlluxioURI(ufsPath);
        // Create ancestor directories from top to the bottom. We cannot use recursive create
        // parents here because the permission for the ancestors can be different.
        Stack<Pair<String, MkdirsOptions>> ufsDirsToMakeWithOptions = new Stack<>();
        AlluxioURI curAlluxioPath = uri.getParent();
        AlluxioURI curUfsPath = dstPath.getParent();
        // Stop at the Alluxio root because the mapped directory of Alluxio root in UFS may not
        // exist.
        while (!ufs.isDirectory(curUfsPath.toString()) && curAlluxioPath != null) {
          URIStatus curDirStatus = fs.getStatus(curAlluxioPath);
          ufsDirsToMakeWithOptions.push(new Pair<>(curUfsPath.toString(),
              MkdirsOptions.defaults().setCreateParent(false).setOwner(curDirStatus.getOwner())
                  .setGroup(curDirStatus.getGroup())
                  .setMode(new Mode((short) curDirStatus.getMode()))));
          curAlluxioPath = curAlluxioPath.getParent();
          curUfsPath = curUfsPath.getParent();
        }
        while (!ufsDirsToMakeWithOptions.empty()) {
          Pair<String, MkdirsOptions> ufsDirAndPerm = ufsDirsToMakeWithOptions.pop();
          // UFS mkdirs might fail if the directory is already created. If so, skip the mkdirs
          // and assume the directory is already prepared, regardless of permission matching.
          if (!ufs.mkdirs(ufsDirAndPerm.getFirst(), ufsDirAndPerm.getSecond())
              && !ufs.isDirectory(ufsDirAndPerm.getFirst())) {
            throw new IOException(
                "Failed to create " + ufsDirAndPerm.getFirst() + " with permission " + ufsDirAndPerm
                    .getSecond().toString());
          }
        }
        URIStatus uriStatus = fs.getStatus(uri);
        OutputStream out = closer.register(
            ufs.create(dstPath.toString(), CreateOptions.defaults().setOwner(uriStatus.getOwner())
                .setGroup(uriStatus.getGroup()).setMode(new Mode((short) uriStatus.getMode()))));
        bytesWritten = IOUtils.copyLarge(in, out);
        incrementPersistedMetric(ufsClient.getUfsMountPointUri(), bytesWritten);
      }
      LOG.info("Persisted file {} with size {}", ufsPath, bytesWritten);
    }
    return null;
  }

  private void incrementPersistedMetric(AlluxioURI ufsMountPointUri, long bytes) {
    String mountPoint = MetricsSystem.escape(ufsMountPointUri);
    String metricName = String.format("BytesPersisted-Ufs:%s", mountPoint);
    MetricsSystem.counter(metricName).inc(bytes);
  }

  @Override
  public Class<PersistConfig> getJobConfigClass() {
    return PersistConfig.class;
  }
}
