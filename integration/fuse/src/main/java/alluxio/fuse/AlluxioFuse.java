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
import alluxio.exception.runtime.FailedPreconditionRuntimeException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.jnifuse.LibFuse;
import alluxio.jnifuse.utils.LibfuseVersion;
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
  private static final String MOUNT_ROOT_UFS_OPTION_NAME = "u";
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
  private static final Option MOUNT_ROOT_UFS_OPTION
      = Option.builder(MOUNT_ROOT_UFS_OPTION_NAME)
      .hasArg()
      .required(false)
      .longOpt("root-ufs")
      .desc("The UFS address to mount to the given Fuse mount point "
          + "(for example, mount ufs address `s3://my_bucket/my_folder` "
          + "to fuse mount point `/mnt/alluxio-fuse`; "
          + "local operations like `mkdir /mnt/alluxio-fuse/folder` will be translated to "
          + "`mkdir s3://my_bucket/my_folder/folder`)")
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
      .addOption(MOUNT_ROOT_UFS_OPTION)
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

    if (cli.hasOption(HELP_OPTION_NAME)) {
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(AlluxioFuse.class.getName(), OPTIONS);
      return;
    }

    LOG.info("Alluxio version: {}-{}", RuntimeConstants.VERSION, ProjectConstants.REVISION);
    setConfigurationFromInput(cli, Configuration.modifiableGlobal());

    AlluxioConfiguration conf = Configuration.global();
    FileSystemContext fsContext = FileSystemContext.create(conf);
    if (!conf.getBoolean(PropertyKey.USER_UFS_ENABLED)) {
      conf = AlluxioFuseUtils.tryLoadingConfigFromMaster(fsContext);
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
      launchFuse(fsContext, fs, true);
    } catch (Throwable t) {
      LOG.error("Failed to launch FUSE", t);
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
      boolean blocking) {
    AlluxioConfiguration conf = fsContext.getClusterConf();
    validateFuseConfiguration(conf);

    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(conf));

    String targetPath = AlluxioFuseUtils.getMountedRootPath(conf);
    String mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    Path mountPath = Paths.get(mountPoint);
    String[] optimizedMountOptions = optimizeAndTransformFuseMountOptions(conf);
    if (!Files.exists(mountPath)) {
      LOG.warn("Mount point on local filesystem does not exist, creating {}", mountPoint);
      try {
        Files.createDirectories(mountPath);
      } catch (IOException e) {
        throw new FailedPreconditionRuntimeException("Failed to create mount point");
      }
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
      } catch (RuntimeException e) {
        fuseFs.umount(true);
        throw e;
      }
    } else {
      final AlluxioJnrFuseFileSystem fuseFs = new AlluxioJnrFuseFileSystem(fs, conf);
      try {
        fuseFs.mount(mountPath, blocking, debugEnabled, optimizedMountOptions);
        return fuseFs;
      } catch (Throwable t) {
        // only try to umount file system when exception occurred.
        // jnr-fuse registers JVM shutdown hook to ensure fs.umount()
        // will be executed when this process is exiting.
        fuseFs.umount();
        throw t;
      }
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
    if (cli.hasOption(MOUNT_ROOT_UFS_OPTION_NAME)) {
      conf.set(PropertyKey.USER_UFS_ENABLED, true, Source.RUNTIME);
      conf.set(PropertyKey.USER_ROOT_UFS,
          cli.getOptionValue(MOUNT_ROOT_UFS_OPTION_NAME), Source.RUNTIME);
      // Disable connections between FUSE and server
      conf.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false, Source.RUNTIME);
      conf.set(PropertyKey.USER_UPDATE_FILE_ACCESSTIME_DISABLED, true, Source.RUNTIME);
    }
    if (cli.hasOption(MOUNT_OPTIONS_OPTION_NAME)) {
      List<String> fuseOptions = new ArrayList<>();
      String[] mountOptionsArray = cli.getOptionValues(MOUNT_OPTIONS_OPTION_NAME);
      for (String opt : mountOptionsArray) {
        String trimedOpt = opt.trim();
        if (trimedOpt.isEmpty()) {
          continue;
        }
        String[] optArray = trimedOpt.split("=");
        if (optArray.length == 1) {
          fuseOptions.add(trimedOpt);
          continue;
        }
        String key = optArray[0];
        String value = optArray[1];
        if (PropertyKey.isValid(key)) {
          PropertyKey propertyKey = PropertyKey.fromString(key);
          conf.set(propertyKey, propertyKey.parseValue(value), Source.RUNTIME);
          LOG.info("Set Alluxio property key({}={}) from command line input", key, value);
        } else if (key.equals("data_cache")) {
          conf.set(PropertyKey.USER_CLIENT_CACHE_ENABLED, true, Source.RUNTIME);
          conf.set(PropertyKey.USER_CLIENT_CACHE_DIRS,
              PropertyKey.USER_CLIENT_CACHE_DIRS.parseValue(value), Source.RUNTIME);
          LOG.info("Set data cache to {} from command line input", value);
        } else if (key.equals("data_cache_size")) {
          conf.set(PropertyKey.USER_CLIENT_CACHE_SIZE,
              PropertyKey.USER_CLIENT_CACHE_SIZE.parseValue(value), Source.RUNTIME);
          LOG.info("Set data cache size as {} from command line input", value);
        } else if (key.equals("metadata_cache_size")) {
          if (value.equals("0")) {
            continue;
          }
          conf.set(PropertyKey.USER_METADATA_CACHE_ENABLED, true, Source.RUNTIME);
          conf.set(PropertyKey.USER_METADATA_CACHE_MAX_SIZE,
              PropertyKey.USER_METADATA_CACHE_MAX_SIZE.parseValue(value), Source.RUNTIME);
          LOG.info("Set metadata cache size as {} from command line input", value);
        } else if (key.equals("metadata_cache_expire")) {
          conf.set(PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME,
              PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME.parseValue(value), Source.RUNTIME);
          LOG.info("Set metadata cache expiration time as {} from command line input", value);
        } else {
          fuseOptions.add(trimedOpt);
        }
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
      throw new InvalidArgumentRuntimeException(
          String.format("%s should be set and should not be empty",
              PropertyKey.FUSE_MOUNT_POINT.getName()));
    }
    if (conf.getBoolean(PropertyKey.USER_UFS_ENABLED)) {
      if (conf.getString(PropertyKey.USER_ROOT_UFS).isEmpty()) {
        throw new InvalidArgumentRuntimeException(String.format(
            "%s should be set and should not be empty when %s is enabled",
            PropertyKey.USER_ROOT_UFS.getName(), PropertyKey.USER_UFS_ENABLED.getName()));
      }
    } else if (conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH).isEmpty()) {
      throw new InvalidArgumentRuntimeException(
          String.format("%s should be set and should not be empty",
              PropertyKey.FUSE_MOUNT_ALLUXIO_PATH.getName()));
    }
    if (Files.isRegularFile(Paths.get(mountPoint))) {
      LOG.error("Mount point {} is not a directory but a file", mountPoint);
      throw new InvalidArgumentRuntimeException("Failed to launch fuse, mount point is a file");
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
    if (AlluxioFuseUtils.getLibfuseVersion(conf) == LibfuseVersion.VERSION_2) {
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
