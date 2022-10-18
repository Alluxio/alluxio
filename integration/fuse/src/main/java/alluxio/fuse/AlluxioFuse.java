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
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.jnifuse.FuseException;
import alluxio.jnifuse.LibFuse;
import alluxio.jnifuse.utils.VersionPreference;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.network.NetworkAddressUtils;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Main entry point to Alluxio-FUSE.
 */
@ThreadSafe
public final class AlluxioFuse {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuse.class);
  private static final CommandLineParser PARSER = new DefaultParser();

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
          + "(for example, mount alluxio path `/alluxio` to fuse mount point `/mnt/alluxio-fuse`; "
          + "local operations like `mkdir /mnt/alluxio-fuse/folder` will be translated to "
          + "`alluxio fs mkdir /alluxio/folder`)")
      .build();
  private static final Option MOUNT_OPTIONS = Option.builder(MOUNT_OPTIONS_OPTION_NAME)
      .valueSeparator(',')
      .required(false)
      .hasArgs()
      .desc("Providing mount options separating by comma. "
          + "Mount options includes operating system mount options, "
          + "many FUSE specific mount options (e.g. direct_io,attr_timeout=10s.allow_other), "
          + "Alluxio property key=value pairs, and Alluxio FUSE special mount options "
          + "data_cache=<local_cache_directory>,data_cache_size=<size>,"
          + "metadata_cache_size=<size>,metadata_cache_expire=<timeout>")
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
  public static void main(String[] args) throws ParseException {
    CommandLine cli = PARSER.parse(OPTIONS, args);

<<<<<<< HEAD
    // Parsing options needs to know which version is being used.
    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(conf));
||||||| 08eab54fdc
    // Parsing options needs to know which version is being used.
    LibFuse.loadLibrary(AlluxioFuseUtils.getVersionPreference(conf));
=======
    if (cli.hasOption(HELP_OPTION_NAME)) {
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(AlluxioFuse.class.getName(), OPTIONS);
      return;
    }

    LOG.info("Alluxio version: {}-{}", RuntimeConstants.VERSION, ProjectConstants.REVISION);
    setConfigurationFromInput(cli, Configuration.modifiableGlobal());
>>>>>>> 16a60f894f802bbe4b9149f122ab2c79cc70be90

    AlluxioConfiguration conf = Configuration.global();
    FileSystemContext fsContext = FileSystemContext.create(conf);
    conf = AlluxioFuseUtils.tryLoadingConfigFromMaster(fsContext);

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
      launchFuse(fsContext, fs, true);
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
   * @param blocking whether the Fuse application is blocking or not
   * @return the Fuse application handler for future Fuse umount operation
   */
  public static FuseUmountable launchFuse(FileSystemContext fsContext, FileSystem fs,
      boolean blocking) throws IOException {
    AlluxioConfiguration conf = fsContext.getClusterConf();
    validateFuseConfiguration(conf);

    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(conf));

    String targetPath = conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH);
    String mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    Path mountPath = Paths.get(mountPoint);
    String[] optimizedMountOptions = optimizeAndTransformFuseMountOptions(conf);
    try {
      if (!Files.exists(mountPath)) {
        LOG.warn("Mount point on local filesystem does not exist, creating {}", mountPoint);
        Files.createDirectories(mountPath);
      }

      final boolean debugEnabled = conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED);
      if (conf.getBoolean(PropertyKey.FUSE_JNIFUSE_ENABLED)) {
        final AlluxioJniFuseFileSystem fuseFs
            = new AlluxioJniFuseFileSystem(fsContext, fs);

        FuseSignalHandler fuseSignalHandler = new FuseSignalHandler(fuseFs);
        Signal.handle(new Signal("TERM"), fuseSignalHandler);

        try {
          LOG.info("Mounting AlluxioJniFuseFileSystem: mount point=\"{}\", OPTIONS=\"{}\"",
              mountPoint, String.join(",", optimizedMountOptions));
          fuseFs.mount(blocking, debugEnabled, optimizedMountOptions);
          return fuseFs;
        } catch (FuseException e) {
          // only try to umount file system when exception occurred.
          // jni-fuse registers JVM shutdown hook to ensure fs.umount()
          // will be executed when this process is exiting.
          String errorMessage = String.format("Failed to mount path %s to mount point %s",
              targetPath, mountPoint);
          LOG.error(errorMessage, e);
          try {
            fuseFs.umount(true);
          } catch (FuseException fe) {
            LOG.error("Failed to unmount Fuse", fe);
          }
          throw new IOException(errorMessage, e);
        }
      } else {
        final AlluxioJnrFuseFileSystem fuseFs = new AlluxioJnrFuseFileSystem(fs, conf);
        try {
          fuseFs.mount(mountPath, blocking, debugEnabled, optimizedMountOptions);
          return fuseFs;
        } catch (ru.serce.jnrfuse.FuseException e) {
          // only try to umount file system when exception occurred.
          // jnr-fuse registers JVM shutdown hook to ensure fs.umount()
          // will be executed when this process is exiting.
          fuseFs.umount();
          throw new IOException(String.format("Failed to mount path %s to mount point %s",
              targetPath, mountPoint), e);
        }
      }
    } catch (Throwable e) {
      throw new IOException("Failed to mount Alluxio file system", e);
    }
  }

  /**
   * Updates Alluxio configuration according to command line input.
   *
   * @param cli the command line inputs
   * @param conf the modifiable configuration to update
   */
  private static void setConfigurationFromInput(CommandLine cli, InstancedConfiguration conf) {
    if (cli.hasOption(MOUNT_POINT_OPTION_NAME)) {
      conf.set(PropertyKey.FUSE_MOUNT_POINT,
          cli.getOptionValue(MOUNT_POINT_OPTION_NAME), Source.RUNTIME);
    }
    if (cli.hasOption(MOUNT_ALLUXIO_PATH_OPTION_NAME)) {
      conf.set(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH,
          cli.getOptionValue(MOUNT_ALLUXIO_PATH_OPTION_NAME), Source.RUNTIME);
    }
    if (cli.hasOption(MOUNT_OPTIONS_OPTION_NAME)) {
      List<String> fuseOptions = new ArrayList<>();
      String[] mountOptionsArray = cli.getOptionValues(MOUNT_OPTIONS_OPTION_NAME);
      for (String opt : mountOptionsArray) {
        String trimedOpt = opt.trim();
        if (trimedOpt.isEmpty()) {
          continue;
        }
        fuseOptions.add(trimedOpt);
      }
      if (!fuseOptions.isEmpty()) {
        conf.set(PropertyKey.FUSE_MOUNT_OPTIONS, fuseOptions, Source.RUNTIME);
        LOG.info("Set fuse mount point options as {} from command line input",
            String.join(",", fuseOptions));
      }
    }
  }

  private static void validateFuseConfiguration(AlluxioConfiguration conf) {
    String mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    if (mountPoint.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("%s should be set and should not be empty",
              PropertyKey.FUSE_MOUNT_POINT.getName()));
    }
    if (conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH).isEmpty()) {
      throw new IllegalArgumentException(
          String.format("%s should be set and should not be empty",
              PropertyKey.FUSE_MOUNT_ALLUXIO_PATH.getName()));
    }
    if (Files.isRegularFile(Paths.get(mountPoint))) {
      LOG.error("Mount point {} is not a directory but a file", mountPoint);
      throw new IllegalArgumentException("Failed to launch fuse, mount point is a file");
    }
  }

  /**
   * Sets default Fuse mount options and transforms format.
   *
   * @param conf the conf to get fuse mount options from
   * @return the transformed fuse mount option
   */
  private static String[] optimizeAndTransformFuseMountOptions(AlluxioConfiguration conf) {
    List<String> options = new ArrayList<>();
    for (String opt : conf.getList(PropertyKey.FUSE_MOUNT_OPTIONS)) {
      if (opt.isEmpty()) {
        continue;
      }
      options.add("-o" + opt);
    }
    if (AlluxioFuseUtils.getVersionPreference(conf) != VersionPreference.VERSION_3) {
      // Without option big_write, the kernel limits a single writing request to 4k.
      // With option big_write, maximum of a single writing request is 128k.
      // See https://github.com/libfuse/libfuse/blob/fuse_2_9_3/ChangeLog#L655-L659,
      // and https://github.com/torvalds/linux/commit/78bb6cb9a890d3d50ca3b02fce9223d3e734ab9b.
      // Libfuse3 dropped this option because it's default
      String bigWritesOptions = "-obig_writes";
      options.add(bigWritesOptions);
      LOG.info("Added fuse mount option {} to enlarge single write request size", bigWritesOptions);
    }
    if (!conf.getBoolean(PropertyKey.FUSE_JNIFUSE_ENABLED)) {
      String directIOOptions = "-odirect_io";
      options.add(directIOOptions);
      LOG.info("Added fuse mount option {} for JNR FUSE", directIOOptions);
    }
    return options.toArray(new String[0]);
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
