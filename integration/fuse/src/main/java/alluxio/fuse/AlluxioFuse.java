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

package alluxio.fuse;

import alluxio.ProjectConstants;
import alluxio.RuntimeConstants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.jnifuse.FuseException;
import alluxio.jnifuse.LibFuse;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.network.NetworkAddressUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Main entry point to Alluxio-FUSE.
 */
@ThreadSafe
public final class AlluxioFuse {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuse.class);

  // prevent instantiation
  private AlluxioFuse() {}

  /**
   * Running this class will mount the file system according to the options passed to this function.
   * The user-space fuse application will stay on the foreground and keep the file system mounted.
   * The user can unmount the file system by gracefully killing (SIGINT) the process.
   *
   * @param args arguments to run the command line
   */
  public static void main(String[] args) {
    LOG.info("Alluxio version: {}-{}", RuntimeConstants.VERSION, ProjectConstants.REVISION);
    AlluxioConfiguration conf = InstancedConfiguration.defaults();
    FileSystemContext fsContext = FileSystemContext.create(conf);
    conf = AlluxioFuseUtils.tryLoadingConfigFromMaster(conf, fsContext);

    final Optional<AlluxioFuseCliOpts> cliOpts = AlluxioFuseCliOpts.AlluxioFuseCliParser
        .parseAndCreateAlluxioFuseCliOpts(args);
    if (!cliOpts.isPresent()) {
      System.exit(1);
    }
    final AlluxioFuseFileSystemOpts fuseFsOpts =
        AlluxioFuseFileSystemOpts.create(conf, cliOpts.get());

    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.CLIENT);
    MetricsSystem.startSinks(conf.getString(PropertyKey.METRICS_CONF_FILE));
    if (conf.getBoolean(PropertyKey.FUSE_WEB_ENABLED)) {
      FuseWebServer webServer = new FuseWebServer(
          NetworkAddressUtils.ServiceType.FUSE_WEB.getServiceName(),
          NetworkAddressUtils.getBindAddress(
              NetworkAddressUtils.ServiceType.FUSE_WEB,
              ServerConfiguration.global()));
      webServer.start();
    }
    startJvmMonitorProcess();
    try (FileSystem fs = FileSystem.Factory.create(fsContext)) {
      FuseUmountable fuseUmountable = launchFuse(fsContext, fs, conf,
          fuseFsOpts, true);
    } catch (IOException e) {
      LOG.error("Failed to launch FUSE", e);
      System.exit(-1);
    }
  }

  /**
   * Launches Fuse application.
   *
   * @param fsContext file system context for Fuse client to communicate to servers
   * @param fs file system for Fuse client to communicate to servers
   * @param conf the alluxio configuration to create Fuse file system
   * @param fuseFsOpts the fuse filesystem options
   * @param blocking whether the Fuse application is blocking or not
   * @return the Fuse application handler for future Fuse umount operation
   */
  public static FuseUmountable launchFuse(FileSystemContext fsContext, FileSystem fs,
      AlluxioConfiguration conf, AlluxioFuseFileSystemOpts fuseFsOpts, boolean blocking)
      throws IOException {

    LibFuse.loadLibrary(AlluxioFuseUtils.getVersionPreference(conf));

    try {
      String mountPoint = fuseFsOpts.getMountPoint();
      Path mountPath = Paths.get(mountPoint);
      if (Files.isRegularFile(mountPath)) {
        LOG.error("Mount point {} is not a directory but a file", mountPoint);
        throw new IOException("Failed to launch fuse, mount point is a file");
      }
      if (!Files.exists(mountPath)) {
        LOG.warn("Mount point on local filesystem does not exist, creating {}", mountPoint);
        Files.createDirectories(mountPath);
      }

      final List<String> fuseOpts = fuseFsOpts.getFuseOptions();
      if (conf.getBoolean(PropertyKey.FUSE_JNIFUSE_ENABLED)) {
        final AlluxioJniFuseFileSystem fuseFs
            = new AlluxioJniFuseFileSystem(fsContext, fs, fuseFsOpts, conf);

        FuseSignalHandler fuseSignalHandler = new FuseSignalHandler(fuseFs);
        Signal.handle(new Signal("TERM"), fuseSignalHandler);

        try {
          String[] fuseOptsArray = fuseOpts.toArray(new String[0]);
          LOG.info("Mounting AlluxioJniFuseFileSystem: mount point=\"{}\", OPTIONS=\"{}\"",
              fuseFsOpts.getMountPoint(), fuseOptsArray);
          fuseFs.mount(blocking, fuseFsOpts.isDebug(), fuseOptsArray);
          return fuseFs;
        } catch (FuseException e) {
          // only try to umount file system when exception occurred.
          // jni-fuse registers JVM shutdown hook to ensure fs.umount()
          // will be executed when this process is exiting.
          String errorMessage = String.format("Failed to mount alluxio path %s to mount point %s",
              fuseFsOpts.getAlluxioPath(), fuseFsOpts.getMountPoint());
          LOG.error(errorMessage, e);
          try {
            fuseFs.umount(true);
          } catch (FuseException fe) {
            LOG.error("Failed to unmount Fuse", fe);
          }
          throw new IOException(errorMessage, e);
        }
      } else {
        // Force direct_io in JNR-FUSE: writes and reads bypass the kernel page
        // cache and go directly to alluxio. This avoids extra memory copies
        // in the write path.
        // TODO(binfan): support kernel_cache (issues#10840)
        fuseOpts.add("-odirect_io");
        final AlluxioJnrFuseFileSystem fuseFs = new AlluxioJnrFuseFileSystem(fs, fuseFsOpts);
        try {
          fuseFs.mount(Paths.get(fuseFsOpts.getMountPoint()), blocking, fuseFsOpts.isDebug(),
              fuseOpts.toArray(new String[0]));
          return fuseFs;
        } catch (ru.serce.jnrfuse.FuseException e) {
          // only try to umount file system when exception occurred.
          // jnr-fuse registers JVM shutdown hook to ensure fs.umount()
          // will be executed when this process is exiting.
          fuseFs.umount();
          throw new IOException(String.format("Failed to mount alluxio path %s to mount point %s",
              fuseFsOpts.getAlluxioPath(), fuseFsOpts.getMountPoint()), e);
        }
      }
    } catch (Throwable e) {
      throw new IOException("Failed to mount Alluxio file system", e);
    }
  }

  /**
   * Starts jvm monitor process, to monitor jvm.
   */
  private static void startJvmMonitorProcess() {
    if (ServerConfiguration.getBoolean(PropertyKey.STANDALONE_FUSE_JVM_MONITOR_ENABLED)) {
      JvmPauseMonitor jvmPauseMonitor = new JvmPauseMonitor(
          ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
          ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS),
          ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS));
      jvmPauseMonitor.start();
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.TOTAL_EXTRA_TIME.getName()),
          jvmPauseMonitor::getTotalExtraTime);
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.INFO_TIME_EXCEEDED.getName()),
          jvmPauseMonitor::getInfoTimeExceeded);
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WARN_TIME_EXCEEDED.getName()),
          jvmPauseMonitor::getWarnTimeExceeded);
    }
  }
}
