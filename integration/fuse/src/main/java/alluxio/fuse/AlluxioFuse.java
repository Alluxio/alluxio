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
import alluxio.conf.Configuration;
import alluxio.jnifuse.FuseException;
import alluxio.jnifuse.LibFuse;
import alluxio.jnifuse.utils.NativeLibraryLoader;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.RetryUtils;
import alluxio.util.CommonUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Main entry point to Alluxio-FUSE.
 */
@ThreadSafe
public final class AlluxioFuse {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuse.class);
  private static final String MOUNT_POINT_OPTION_NAME = "m";
  private static final String MOUNT_ALLUXIO_PATH_OPTION_NAME = "a";
  private static final String MOUNT_OPTIONS_OPTION_NAME = "o";
  private static final String HELP_OPTION_NAME = "h";

  private static final Option MOUNT_POINT_OPTION = Option.builder(MOUNT_POINT_OPTION_NAME)
      .hasArg()
      .required(false)
      .longOpt("mount-point")
      .desc("The absolute local filesystem path that standalone Fuse will mount Alluxio path to.")
      .build();

  private static final Option MOUNT_ALLUXIO_PATH_OPTION
      = Option.builder(MOUNT_ALLUXIO_PATH_OPTION_NAME)
      .hasArg()
      .required(false)
      .longOpt("alluxio-path")
      .desc("The Alluxio path to mount to the given Fuse mount point "
          + "(e.g., /users/foo; defaults to /)")
      .build();

  private static final Option MOUNT_OPTIONS = Option.builder(MOUNT_OPTIONS_OPTION_NAME)
      .valueSeparator(',')
      .required(false)
      .hasArgs()
      .desc("FUSE mount options")
      .build();

  private static final Option HELP_OPTION = Option.builder(HELP_OPTION_NAME)
      .required(false)
      .desc("Print this help message")
      .build();

  private static final Options OPTIONS = new Options()
      .addOption(MOUNT_POINT_OPTION)
      .addOption(MOUNT_ALLUXIO_PATH_OPTION)
      .addOption(MOUNT_OPTIONS)
      .addOption(HELP_OPTION);

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

    // Parsing options needs to know which version is being used.
    LibFuse.loadLibrary(AlluxioFuseUtils.getVersionPreference(conf));

    FileSystemContext fsContext = FileSystemContext.create(conf);
    try {
      InetSocketAddress confMasterAddress =
          fsContext.getMasterClientContext().getConfMasterInquireClient().getPrimaryRpcAddress();
      RetryUtils.retry("load cluster default configuration with master " + confMasterAddress,
          () -> fsContext.getClientContext().loadConfIfNotLoaded(confMasterAddress),
          RetryUtils.defaultClientRetry(
              conf.getDuration(PropertyKey.USER_RPC_RETRY_MAX_DURATION),
              conf.getDuration(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS),
              conf.getDuration(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS)));
    } catch (IOException e) {
      LOG.warn("Failed to load cluster default configuration for Fuse process. "
          + "Proceed with local configuration for FUSE: {}", e.toString());
    }
    conf = fsContext.getClusterConf();

    final FuseMountConfig config = createFuseMountConfig(args, conf);
    if (config == null) {
      System.exit(1);
    }

    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.CLIENT);
    MetricsSystem.startSinks(conf.getString(PropertyKey.METRICS_CONF_FILE));
    if (conf.getBoolean(PropertyKey.FUSE_WEB_ENABLED)) {
      FuseWebServer webServer = new FuseWebServer(
          NetworkAddressUtils.ServiceType.FUSE_WEB.getServiceName(),
          NetworkAddressUtils.getBindAddress(
              NetworkAddressUtils.ServiceType.FUSE_WEB,
              Configuration.global()));
      webServer.start();
    }
    startJvmMonitorProcess();
    try (FileSystem fs = FileSystem.Factory.create(fsContext)) {
      FuseUmountable fuseUmountable = launchFuse(fsContext, fs, conf, config, true);
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
   * @param mountConfig the fuse mount configuration
   * @param blocking whether the Fuse application is blocking or not
   * @return the Fuse application handler for future Fuse umount operation
   */
  public static FuseUmountable launchFuse(FileSystemContext fsContext, FileSystem fs,
      AlluxioConfiguration conf, FuseMountConfig mountConfig, boolean blocking) throws IOException {
    Preconditions.checkNotNull(mountConfig,
        "Fuse mount configuration should not be null to launch a Fuse application");

    // There are other entries to this method other than the main function above
    // It is ok to call this function multiple times.
    LibFuse.loadLibrary(AlluxioFuseUtils.getVersionPreference(conf));

    try {
      String mountPoint = mountConfig.getMountPoint();
      Path mountPath = Paths.get(mountPoint);
      if (Files.isRegularFile(mountPath)) {
        LOG.error("Mount point {} is not a directory but a file", mountPoint);
        throw new IOException("Failed to launch fuse, mount point is a file");
      }
      if (!Files.exists(mountPath)) {
        LOG.warn("Mount point on local filesystem does not exist, creating {}", mountPoint);
        Files.createDirectories(mountPath);
      }

      final List<String> fuseOpts = mountConfig.getFuseMountOptions();
      if (conf.getBoolean(PropertyKey.FUSE_JNIFUSE_ENABLED)) {
        final AlluxioJniFuseFileSystem fuseFs
            = new AlluxioJniFuseFileSystem(fsContext, fs, mountConfig, conf);

        FuseSignalHandler fuseSignalHandler = new FuseSignalHandler(fuseFs);
        Signal.handle(new Signal("TERM"), fuseSignalHandler);

        try {
          String[] fuseOptsArray = fuseOpts.toArray(new String[0]);
          LOG.info("Mounting AlluxioJniFuseFileSystem: mount point=\"{}\", OPTIONS=\"{}\"",
              mountConfig.getMountPoint(), fuseOptsArray);
          fuseFs.mount(blocking, mountConfig.isDebug(), fuseOptsArray);
          return fuseFs;
        } catch (FuseException e) {
          // only try to umount file system when exception occurred.
          // jni-fuse registers JVM shutdown hook to ensure fs.umount()
          // will be executed when this process is exiting.
          String errorMessage = String.format("Failed to mount alluxio path %s to mount point %s",
              mountConfig.getMountAlluxioPath(), mountConfig.getMountPoint());
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
        final AlluxioFuseFileSystem fuseFs = new AlluxioFuseFileSystem(fs, mountConfig, conf);
        try {
          fuseFs.mount(Paths.get(mountConfig.getMountPoint()), blocking, mountConfig.isDebug(),
              fuseOpts.toArray(new String[0]));
          return fuseFs;
        } catch (ru.serce.jnrfuse.FuseException e) {
          // only try to umount file system when exception occurred.
          // jnr-fuse registers JVM shutdown hook to ensure fs.umount()
          // will be executed when this process is exiting.
          fuseFs.umount();
          throw new IOException(String.format("Failed to mount alluxio path %s to mount point %s",
              mountConfig.getMountAlluxioPath(), mountConfig.getMountPoint()), e);
        }
      }
    } catch (Throwable e) {
      throw new IOException("Failed to mount Alluxio file system", e);
    }
  }

  /**
   * Parses CLI options and creates fuse mount configuration.
   *
   * @param args CLI args
   * @return Alluxio-FUSE configuration options
   */
  @Nullable
  private static FuseMountConfig createFuseMountConfig(String[] args,
      AlluxioConfiguration alluxioConf) {
    final CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cli = parser.parse(OPTIONS, args);

      if (cli.hasOption("h")) {
        final HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp(AlluxioFuse.class.getName(), OPTIONS);
        return null;
      }

      String mountPoint = cli.getOptionValue(MOUNT_POINT_OPTION_NAME);
      String mountAlluxioPath = cli.getOptionValue(MOUNT_ALLUXIO_PATH_OPTION_NAME);
      List<String> fuseOpts = cli.hasOption(MOUNT_OPTIONS_OPTION_NAME)
          ? Arrays.asList(cli.getOptionValues(MOUNT_OPTIONS_OPTION_NAME)) : null;

      return FuseMountConfig.create(mountPoint, mountAlluxioPath, fuseOpts, alluxioConf);
    } catch (ParseException e) {
      System.err.println("Error while parsing CLI: " + e.getMessage());
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(AlluxioFuse.class.getName(), OPTIONS);
      return null;
    }
  }

  /**
   * Parses user given fuse options to the format that Fuse application needs.
   *
   * @param fuseOptions the fuse options to parse from
   * @param alluxioConf alluxio configuration
   * @return the parsed fuse options
   */
  public static List<String> parseFuseOptions(List<String> fuseOptions,
      AlluxioConfiguration alluxioConf) {
    Preconditions.checkNotNull(fuseOptions, "fuse options");
    Preconditions.checkNotNull(alluxioConf, "alluxio configuration");
    boolean using3 =
        NativeLibraryLoader.getLoadState().equals(NativeLibraryLoader.LoadState.LOADED_3);

    List<String> res = new ArrayList<>();
    if (!using3) {
      res.add("-obig_writes");
    }
    boolean noUserMaxWrite = true;
    for (final String opt : fuseOptions) {
      if (opt.isEmpty()) {
        continue;
      }

      // libfuse3 has dropped big_writes
      if (using3 && opt.equals("big_writes")) {
        continue;
      }

      res.add("-o" + opt);

      if (noUserMaxWrite && opt.startsWith("max_write")) {
        noUserMaxWrite = false;
      }
    }
    // check if the user has specified his own max_write, otherwise get it
    // from conf
    if (noUserMaxWrite) {
      final long maxWrite = alluxioConf.getBytes(PropertyKey.FUSE_MAXWRITE_BYTES);
      res.add(String.format("-omax_write=%d", maxWrite));
    }

    return res;
  }

  /**
   * Starts jvm monitor process, to monitor jvm.
   */
  private static void startJvmMonitorProcess() {
    if (Configuration.getBoolean(PropertyKey.STANDALONE_FUSE_JVM_MONITOR_ENABLED)) {
      JvmPauseMonitor jvmPauseMonitor = new JvmPauseMonitor(
          Configuration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
          Configuration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS),
          Configuration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS));
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
