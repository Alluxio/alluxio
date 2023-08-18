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

import static alluxio.fuse.options.FuseOptions.FUSE_UPDATE_CHECK_ENABLED;

import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.RuntimeConstants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.runtime.FailedPreconditionRuntimeException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.fuse.meta.UpdateChecker;
import alluxio.fuse.options.FuseCliOptions;
import alluxio.fuse.options.FuseOptions;
import alluxio.fuse.options.MountOptions;
import alluxio.heartbeat.FixedIntervalSupplier;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.jnifuse.LibFuse;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.user.UserState;
import alluxio.util.CommonUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.network.NetworkAddressUtils;

import com.beust.jcommander.JCommander;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Main entry point to Alluxio-FUSE.
 */
@ThreadSafe
public class AlluxioFuse {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuse.class);

  // prevent instantiation
  protected AlluxioFuse() {
  }

  /**
   * Startup the FUSE process.
   *
   * @param conf configuration
   * @throws ParseException
   */
  public void start(AlluxioConfiguration conf) throws ParseException {
    FuseOptions fuseOptions = FuseOptions.Builder.fromConfig(conf).build();

    FileSystemContext fsContext = FileSystemContext.create(conf);
    if (!fuseOptions.getFileSystemOptions().getUfsFileSystemOptions().isPresent()
        && !fuseOptions.getFileSystemOptions().isDoraCacheEnabled()) {
      // cases other than standalone fuse sdk
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
    ExecutorService executor = null;
    if (fuseOptions.updateCheckEnabled()) {
      executor = Executors.newSingleThreadExecutor();
      executor.submit(new HeartbeatThread(HeartbeatContext.FUSE_UPDATE_CHECK,
          UpdateChecker.create(fuseOptions), () -> new FixedIntervalSupplier(Constants.DAY_MS),
          Configuration.global(), UserState.Factory.create(conf)));
    }
    try (FileSystem fs = createBaseFileSystem(fsContext, fuseOptions)) {
      AlluxioJniFuseFileSystem fuseFileSystem = createFuseFileSystem(fsContext, fs, fuseOptions);
      setupFuseFileSystem(fuseFileSystem);
      launchFuse(fuseFileSystem, fsContext, fuseOptions, true);
    } catch (Throwable t) {
      if (executor != null) {
        executor.shutdown();
      }
      // TODO(lu) FUSE unmount gracefully
      LOG.error("Failed to launch FUSE", t);
      System.exit(-1);
    }
  }

  /**
   * Running this class will mount the file system according to the options passed to this function.
   * The user-space fuse application will stay on the foreground and keep the file system mounted.
   * The user can unmount the file system by gracefully killing (SIGINT) the process.
   *
   * @param args arguments to run the command line
   */
  public static void main(String[] args) throws ParseException {
    AlluxioFuse alluxioFuse = new AlluxioFuse();
    FuseCliOptions fuseCliOptions = new FuseCliOptions();
    JCommander jCommander = JCommander.newBuilder()
        .addObject(fuseCliOptions)
        .build();
    jCommander.parse(args);
    if (fuseCliOptions.getHelp().orElse(false)) {
      jCommander.usage();
      return;
    }

    LOG.info("Alluxio version: {}-{}", RuntimeConstants.VERSION, ProjectConstants.REVISION);
    InstancedConfiguration configFromCli = parseCliOptionsAsConfig(fuseCliOptions);
    { // !!! we are fiddling with mutable global states here !!!
      // use an explicit scope to limit the visibility of `globalConf`
      InstancedConfiguration globalConf = Configuration.modifiableGlobal();
      globalConf.merge(configFromCli.getProperties());
      Source metadataCacheSizeSource =
          globalConf.getSource(PropertyKey.USER_METADATA_CACHE_MAX_SIZE);
      if (metadataCacheSizeSource == Source.DEFAULT
          || metadataCacheSizeSource == Source.CLUSTER_DEFAULT) {
        globalConf.set(PropertyKey.USER_METADATA_CACHE_MAX_SIZE, 20000, Source.RUNTIME);
        LOG.info("Set default metadata cache size to 20,000 entries "
            + "with around 40MB memory consumption for FUSE");
      }
    }
    AlluxioConfiguration conf = Configuration.global();
    alluxioFuse.start(conf);
  }

  protected FileSystem createBaseFileSystem(FileSystemContext fsContext, FuseOptions fuseOptions) {
    return FileSystem.Factory.create(fsContext, fuseOptions.getFileSystemOptions());
  }

  /**
   * Create a FuseFileSystem instance.
   *
   * @param fsContext the context of the file system on which FuseFileSystem based on
   * @param fs the file system on which FuseFileSystem based on
   * @param fuseOptions the fuse options
   * @return a FuseFileSystem instance
   */
  public AlluxioJniFuseFileSystem createFuseFileSystem(FileSystemContext fsContext, FileSystem fs,
      FuseOptions fuseOptions) {
    AlluxioConfiguration conf = fsContext.getClusterConf();
    validateFuseConfAndOptions(conf, fuseOptions);

    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(conf));

    String mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    Path mountPath = Paths.get(mountPoint);
    if (!Files.exists(mountPath)) {
      LOG.warn("Mount point on local filesystem does not exist, creating {}", mountPoint);
      try {
        Files.createDirectories(mountPath);
      } catch (IOException e) {
        throw new FailedPreconditionRuntimeException("Failed to create mount point");
      }
    }

    return createJniFuseSystem(fsContext, fs, fuseOptions);
  }

  protected AlluxioJniFuseFileSystem createJniFuseSystem(
      FileSystemContext fsContext, FileSystem fs, FuseOptions fuseOptions) {
    return new AlluxioJniFuseFileSystem(fsContext, fs, fuseOptions);
  }

  /**
   * Set up the FUSE file system {@link AlluxioJniFuseFileSystem} before mounting.
   *
   * @param jniFuseFileSystem the instance of {@link AlluxioJniFuseFileSystem}
   */
  public void setupFuseFileSystem(AlluxioJniFuseFileSystem jniFuseFileSystem) {
    // do nothing by default
  }

  /**
   * Launches Fuse application.
   *
   * @param fuseFs the fuse file system
   * @param fsContext file system context for Fuse client to communicate to servers
   * @param fuseOptions Fuse options
   * @param blocking whether the Fuse application is blocking or not
   * @return the Fuse application handler for future Fuse umount operation
   */
  public static AlluxioJniFuseFileSystem launchFuse(AlluxioJniFuseFileSystem fuseFs,
      FileSystemContext fsContext, FuseOptions fuseOptions,
      boolean blocking) {
    AlluxioConfiguration conf = fsContext.getClusterConf();
    validateFuseConfAndOptions(conf, fuseOptions);

    String mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    final boolean debugEnabled = conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED);
    FuseSignalHandler fuseSignalHandler = new FuseSignalHandler(fuseFs);
    Signal.handle(new Signal("TERM"), fuseSignalHandler);
    try {
      LOG.info("Mounting AlluxioJniFuseFileSystem: mount point=\"{}\", OPTIONS=\"{}\"",
          mountPoint, String.join(",", fuseOptions.getFuseMountOptions()));
      fuseFs.mount(blocking, debugEnabled, fuseOptions.getFuseMountOptions());
      return fuseFs;
    } catch (RuntimeException e) {
      fuseFs.umount(true);
      throw e;
    }
  }

  /**
   * Converts command line options into Alluxio configuration.
   *
   * @param cli the command line inputs
   * @return configuration containing the properties parsed from cli
   */
  protected static InstancedConfiguration parseCliOptionsAsConfig(FuseCliOptions cli) {
    InstancedConfiguration conf = new InstancedConfiguration(new AlluxioProperties());
    cli.getMountPoint()
        .ifPresent(mp -> conf.set(PropertyKey.FUSE_MOUNT_POINT, mp, Source.RUNTIME));
    cli.getUpdateCheck()
        .ifPresent(updateCheckEnabled -> {
          conf.set(FUSE_UPDATE_CHECK_ENABLED, updateCheckEnabled, Source.RUNTIME);
        });
    cli.getRootUfsUri()
        .ifPresent(ufsRootUri -> {
          conf.set(FuseOptions.FUSE_UFS_ROOT, ufsRootUri.toString(), Source.RUNTIME);
          // Disable connections between FUSE and server
          conf.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false, Source.RUNTIME);
          conf.set(PropertyKey.USER_UPDATE_FILE_ACCESSTIME_DISABLED, true, Source.RUNTIME);
        });
    Optional<MountOptions> mountOptions = cli.getMountOptions();
    mountOptions.map(MountOptions::getAlluxioOptions)
        .ifPresent(alluxioOptions -> alluxioOptions.forEach((key, value) -> {
          PropertyKey propertyKey = PropertyKey.fromString(key);
          conf.set(propertyKey, propertyKey.parseValue(value), Source.RUNTIME);
          LOG.info("Set Alluxio property key({}={}) from command line input", key, value);
        }));
    mountOptions.flatMap(MountOptions::getFuseVersion)
        .ifPresent(fuseVersion -> {
          conf.set(PropertyKey.FUSE_JNIFUSE_LIBFUSE_VERSION,
              PropertyKey.FUSE_JNIFUSE_LIBFUSE_VERSION.parseValue(fuseVersion), Source.RUNTIME);
          LOG.info("Set libfuse version to {} from command line input", fuseVersion);
        });
    mountOptions.flatMap(MountOptions::getDataCacheDirs)
        .ifPresent(dataCacheDirs -> {
          conf.set(PropertyKey.USER_CLIENT_CACHE_ENABLED, true, Source.RUNTIME);
          conf.set(PropertyKey.USER_CLIENT_CACHE_DIRS,
              PropertyKey.USER_CLIENT_CACHE_DIRS.parseValue(dataCacheDirs), Source.RUNTIME);
          LOG.info("Set data cache to {} from command line input", dataCacheDirs);
        });
    mountOptions.flatMap(MountOptions::getDataCacheSizes)
        .ifPresent(dataCacheSizes -> {
          conf.set(PropertyKey.USER_CLIENT_CACHE_SIZE,
              PropertyKey.USER_CLIENT_CACHE_SIZE.parseValue(dataCacheSizes), Source.RUNTIME);
          LOG.info("Set data cache size as {} from command line input", dataCacheSizes);
        });
    mountOptions.flatMap(MountOptions::getMetadataCacheSize)
        .ifPresent(metadataCacheSize -> {
          conf.set(PropertyKey.USER_METADATA_CACHE_MAX_SIZE,
              PropertyKey.USER_METADATA_CACHE_MAX_SIZE.parseValue(metadataCacheSize),
              Source.RUNTIME);
          LOG.info("Set metadata cache size as {} from command line input", metadataCacheSize);
        });
    mountOptions.flatMap(MountOptions::getMetadataCacheExpireTime)
        .ifPresent(expireTime -> {
          conf.set(PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME,
              PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME.parseValue(expireTime),
              Source.RUNTIME);
          LOG.info("Set metadata cache expiration time as {} from command line input", expireTime);
        });
    mountOptions.map(MountOptions::getUnrecognizedOptions)
        .ifPresent(options -> {
          List<String> fuseOptions = options.entrySet()
              .stream()
              .map(entry -> entry.getKey()
                  + (entry.getValue().isEmpty() ? "" : "=" + entry.getValue()))
              .collect(Collectors.toList());
          conf.set(PropertyKey.FUSE_MOUNT_OPTIONS, fuseOptions, Source.RUNTIME);
          LOG.info("Set fuse mount point options as {} from command line input",
              String.join(",", fuseOptions));
        });
    return conf;
  }

  protected static void validateFuseConfAndOptions(AlluxioConfiguration conf, FuseOptions options) {
    String mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    if (mountPoint.isEmpty()) {
      throw new InvalidArgumentRuntimeException(
          String.format("%s should be set and should not be empty",
              PropertyKey.FUSE_MOUNT_POINT.getName()));
    }
    if (!options.getFileSystemOptions().getUfsFileSystemOptions().isPresent()
        && conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH).isEmpty()) {
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
   * Starts jvm monitor process, to monitor jvm.
   */
  protected static void startJvmMonitorProcess() {
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
