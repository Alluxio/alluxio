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

package alluxio.job.plan.persist;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.job.plan.AbstractVoidPlanDefinition;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.util.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job that persists a file into the under storage.
 */
@NotThreadSafe
public final class PersistDefinition
    extends AbstractVoidPlanDefinition<PersistConfig, SerializableVoid> {
  private static final Logger LOG = LoggerFactory.getLogger(PersistDefinition.class);

  /**
   * Constructs a new {@link PersistDefinition}.
   */
  public PersistDefinition() {
  }

  @Override
  public Set<Pair<WorkerInfo, SerializableVoid>> selectExecutors(PersistConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context)
      throws Exception {
    if (jobWorkerInfoList.isEmpty()) {
      throw new RuntimeException("No worker is available");
    }

    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    List<BlockWorkerInfo> alluxioWorkerInfoList = context.getFsContext().getCachedWorkers();
    BlockWorkerInfo workerWithMostBlocks = JobUtils.getWorkerWithMostBlocks(alluxioWorkerInfoList,
        context.getFileSystem().getStatus(uri).getFileBlockInfos());

    // Map the best Alluxio worker to a job worker.
    Set<Pair<WorkerInfo, SerializableVoid>> result = Sets.newHashSet();
    boolean found = false;
    if (workerWithMostBlocks != null) {
      for (WorkerInfo workerInfo : jobWorkerInfoList) {
        if (workerInfo.getAddress().getHost()
            .equals(workerWithMostBlocks.getNetAddress().getHost())) {
          result.add(new Pair<>(workerInfo, null));
          found = true;
          break;
        }
      }
    }
    if (!found) {
      result.add(new Pair<>(
          jobWorkerInfoList.get(new Random().nextInt(jobWorkerInfoList.size())), null));
    }
    return result;
  }

  @Override
  public SerializableVoid runTask(PersistConfig config, SerializableVoid args,
      RunTaskContext context) throws Exception {
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
          ufs.deleteExistingFile(ufsPath);
        } else {
          throw new IOException("File " + config.getFilePath()
              + " is already persisted in UFS, to overwrite the file, please set the overwrite flag"
              + " in the config.");
        }
      }

      URIStatus uriStatus = context.getFileSystem().getStatus(uri);
      if (!uriStatus.isCompleted()) {
        throw new IOException("Cannot persist an incomplete Alluxio file: " + uri);
      }
      long bytesWritten;
      try (Closer closer = Closer.create()) {
        OpenFilePOptions options = OpenFilePOptions.newBuilder()
            .setReadType(ReadPType.NO_CACHE)
            .setUpdateLastAccessTime(false)
            .build();
        FileInStream in = closer.register(context.getFileSystem().openFile(uri, options));
        AlluxioURI dstPath = new AlluxioURI(ufsPath);
        // Create ancestor directories from top to the bottom. We cannot use recursive create
        // parents here because the permission for the ancestors can be different.
        Stack<Pair<String, String>> ancestorUfsAndAlluxioPaths = new Stack<>();
        AlluxioURI curAlluxioPath = uri.getParent();
        AlluxioURI curUfsPath = dstPath.getParent();
        // Stop at the Alluxio root because the mapped directory of Alluxio root in UFS may not
        // exist.
        while (!ufs.isDirectory(curUfsPath.toString()) && curAlluxioPath != null) {
          ancestorUfsAndAlluxioPaths.push(
              new Pair<>(curUfsPath.toString(), curAlluxioPath.toString()));

          curAlluxioPath = curAlluxioPath.getParent();
          curUfsPath = curUfsPath.getParent();
        }
        while (!ancestorUfsAndAlluxioPaths.empty()) {
          Pair<String, String> ancestorUfsAndAlluxioPath = ancestorUfsAndAlluxioPaths.pop();
          String ancestorUfsPath = ancestorUfsAndAlluxioPath.getFirst();
          String ancestorAlluxioPath = ancestorUfsAndAlluxioPath.getSecond();
          URIStatus status = context.getFileSystem().getStatus(new AlluxioURI(ancestorAlluxioPath));
          MkdirsOptions mkdirOptions = MkdirsOptions.defaults(ServerConfiguration.global())
              .setCreateParent(false)
              .setOwner(status.getOwner())
              .setGroup(status.getGroup())
              .setMode(new Mode((short) status.getMode()));
          // UFS mkdirs might fail if the directory is already created. If so, skip the mkdirs
          // and assume the directory is already prepared, regardless of permission matching.
          if (ufs.mkdirs(ancestorUfsPath, mkdirOptions)) {
            List<AclEntry> allAcls = Stream.concat(status.getDefaultAcl().getEntries().stream(),
                status.getAcl().getEntries().stream()).collect(Collectors.toList());
            ufs.setAclEntries(ancestorUfsPath, allAcls);
          } else if (!ufs.isDirectory(ancestorUfsPath)) {
            throw new IOException(
                "Failed to create " + ufsPath + " with permission " + options.toString()
                + " because its ancestor " + ancestorUfsPath + " is not a directory");
          }
        }
        OutputStream out = closer.register(
            ufs.createNonexistingFile(dstPath.toString(),
                CreateOptions.defaults(ServerConfiguration.global()).setOwner(uriStatus.getOwner())
                .setGroup(uriStatus.getGroup()).setMode(new Mode((short) uriStatus.getMode()))));
        URIStatus status = context.getFileSystem().getStatus(uri);
        List<AclEntry> allAcls = Stream.concat(status.getDefaultAcl().getEntries().stream(),
            status.getAcl().getEntries().stream()).collect(Collectors.toList());
        ufs.setAclEntries(dstPath.toString(), allAcls);
        bytesWritten = IOUtils.copyLarge(in, out, new byte[8 * Constants.MB]);
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
