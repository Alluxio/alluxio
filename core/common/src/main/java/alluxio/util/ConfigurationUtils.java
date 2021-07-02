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

package alluxio.util;

import static java.util.stream.Collectors.toList;

import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.cli.CommandUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.conf.path.PathConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GetConfigurationPResponse;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.MetaMasterConfigurationServiceGrpc;
import alluxio.grpc.Scope;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Utilities for working with Alluxio configurations.
 */
public final class ConfigurationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtils.class);

  @GuardedBy("DEFAULT_PROPERTIES_LOCK")
  private static volatile AlluxioProperties sDefaultProperties = null;
  private static String sSourcePropertyFile = null;

  private static final Object DEFAULT_PROPERTIES_LOCK = new Object();
  private static final String MASTERS = "masters";
  private static final String WORKERS = "workers";

  private ConfigurationUtils() {} // prevent instantiation

  /**
   * Gets the embedded journal addresses to use for the given service type (either master-raft or
   * job-master-raft).
   *
   * @param conf configuration
   * @param serviceType the service to get addresses for
   * @return the addresses
   */
  public static List<InetSocketAddress> getEmbeddedJournalAddresses(AlluxioConfiguration conf,
      ServiceType serviceType) {
    Preconditions.checkState(
        serviceType == ServiceType.MASTER_RAFT || serviceType == ServiceType.JOB_MASTER_RAFT);
    if (serviceType == ServiceType.MASTER_RAFT) {
      return getMasterEmbeddedJournalAddresses(conf);
    }
    return getJobMasterEmbeddedJournalAddresses(conf);
  }

  /**
   * @param conf configuration
   * @return the embedded journal addresses to use for the master
   */
  public static List<InetSocketAddress> getMasterEmbeddedJournalAddresses(
      AlluxioConfiguration conf) {
    PropertyKey property = PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES;
    if (conf.isSet(property)) {
      return parseInetSocketAddresses(conf.getList(property, ","));
    }
    // Fall back on master_hostname:master_raft_port
    return Arrays.asList(NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RAFT, conf));
  }

  /**
   * @param conf configuration
   * @return the embedded journal addresses to use for the job master
   */
  public static List<InetSocketAddress> getJobMasterEmbeddedJournalAddresses(
      AlluxioConfiguration conf) {
    PropertyKey jobMasterProperty = PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES;
    if (conf.isSet(jobMasterProperty)) {
      return parseInetSocketAddresses(conf.getList(jobMasterProperty, ","));
    }
    // Fall back on using the master embedded journal addresses, with the job master port.
    PropertyKey masterProperty = PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES;
    int jobRaftPort = NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_RAFT, conf);
    if (conf.isSet(masterProperty)) {
      return overridePort(getMasterEmbeddedJournalAddresses(conf), jobRaftPort);
    }
    // Fall back on job_master_hostname:job_master_raft_port.
    return Arrays.asList(NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_RAFT, conf));
  }

  /**
   * Gets the RPC addresses of all masters based on the configuration.
   *
   * @param conf the configuration to use
   * @return the master rpc addresses
   */
  public static List<InetSocketAddress> getMasterRpcAddresses(AlluxioConfiguration conf) {
    // First check whether rpc addresses are explicitly configured.
    if (conf.isSet(PropertyKey.MASTER_RPC_ADDRESSES)) {
      return parseInetSocketAddresses(conf.getList(PropertyKey.MASTER_RPC_ADDRESSES, ","));
    }

    // Fall back on server-side journal configuration.
    int rpcPort = NetworkAddressUtils.getPort(NetworkAddressUtils.ServiceType.MASTER_RPC, conf);
    return overridePort(getEmbeddedJournalAddresses(conf, ServiceType.MASTER_RAFT), rpcPort);
  }

  /**
   * Gets the RPC addresses of all job masters based on the configuration.
   *
   * @param conf the configuration to use
   * @return the job master rpc addresses
   */
  public static List<InetSocketAddress> getJobMasterRpcAddresses(AlluxioConfiguration conf) {
    // First check whether job rpc addresses are explicitly configured.
    if (conf.isSet(PropertyKey.JOB_MASTER_RPC_ADDRESSES)) {
      return parseInetSocketAddresses(
          conf.getList(PropertyKey.JOB_MASTER_RPC_ADDRESSES, ","));
    }

    int jobRpcPort =
        NetworkAddressUtils.getPort(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC, conf);
    // Fall back on explicitly configured regular master rpc addresses.
    if (conf.isSet(PropertyKey.MASTER_RPC_ADDRESSES)) {
      List<InetSocketAddress> addrs =
          parseInetSocketAddresses(conf.getList(PropertyKey.MASTER_RPC_ADDRESSES, ","));
      return overridePort(addrs, jobRpcPort);
    }

    // Fall back on server-side journal configuration.
    return overridePort(getEmbeddedJournalAddresses(conf, ServiceType.JOB_MASTER_RAFT), jobRpcPort);
  }

  private static List<InetSocketAddress> overridePort(List<InetSocketAddress> addrs, int port) {
    return StreamUtils.map(addr -> InetSocketAddress.createUnresolved(addr.getHostString(), port),
        addrs);
  }

  /**
   * @param addresses a list of address strings in the form "hostname:port"
   * @return a list of InetSocketAddresses representing the given address strings
   */
  private static List<InetSocketAddress> parseInetSocketAddresses(List<String> addresses) {
    List<InetSocketAddress> inetSocketAddresses = new ArrayList<>(addresses.size());
    for (String address : addresses) {
      try {
        inetSocketAddresses.add(NetworkAddressUtils.parseInetSocketAddress(address));
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to parse host:port: " + address, e);
      }
    }
    return inetSocketAddresses;
  }

  /**
   * Loads properties from a resource.
   *
   * @param resource url of the properties file
   * @return a set of properties on success, or null if failed
   */
  @Nullable
  public static Properties loadPropertiesFromResource(URL resource) {
    try (InputStream stream = resource.openStream()) {
      return loadProperties(stream);
    } catch (IOException e) {
      LOG.warn("Failed to read properties from {}: {}", resource, e.toString());
      return null;
    }
  }

  /**
   * Loads properties from the given file.
   *
   * @param filePath the absolute path of the file to load properties
   * @return a set of properties on success, or null if failed
   */
  @Nullable
  public static Properties loadPropertiesFromFile(String filePath) {
    try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
      return loadProperties(fileInputStream);
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      LOG.warn("Failed to close property input stream from {}: {}", filePath, e.toString());
      return null;
    }
  }

  /**
   * @param stream the stream to read properties from
   * @return a properties object populated from the stream
   */
  @Nullable
  public static Properties loadProperties(InputStream stream) {
    Properties properties = new Properties();
    try {
      properties.load(stream);
    } catch (IOException e) {
      LOG.warn("Unable to load properties: {}", e.toString());
      return null;
    }
    return properties;
  }

  /**
   * Searches the given properties file from a list of paths.
   *
   * @param propertiesFile the file to load properties
   * @param confPathList a list of paths to search the propertiesFile
   * @return the site properties file on success search, or null if failed
   */
  @Nullable
  public static String searchPropertiesFile(String propertiesFile,
      String[] confPathList) {
    if (propertiesFile == null || confPathList == null) {
      return null;
    }
    for (String path : confPathList) {
      String file = PathUtils.concatPath(path, propertiesFile);
      Properties properties = loadPropertiesFromFile(file);
      if (properties != null) {
        // If a site conf is successfully loaded, stop trying different paths.
        return file;
      }
    }
    return null;
  }

  /**
   * @param conf the configuration to use
   * @return whether the configuration describes how to find the job master host, either through
   *         explicit configuration or through zookeeper
   */
  public static boolean jobMasterHostConfigured(AlluxioConfiguration conf) {
    boolean usingZk = conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)
        && conf.isSet(PropertyKey.ZOOKEEPER_ADDRESS);
    return conf.isSet(PropertyKey.JOB_MASTER_HOSTNAME) || usingZk
        || getJobMasterRpcAddresses(conf).size() > 1;
  }

  /**
   * Returns a unified message for cases when the master hostname cannot be determined.
   *
   * @param serviceName the name of the service that couldn't run. i.e. Alluxio worker, fsadmin
   *                    shell, etc.
   * @return a string with the message
   */
  public static String getMasterHostNotConfiguredMessage(String serviceName) {
    return getHostNotConfiguredMessage(serviceName, "master", PropertyKey.MASTER_HOSTNAME,
        PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES);
  }

  /**
   * Returns a unified message for cases when the job master hostname cannot be determined.
   *
   * @param serviceName the name of the service that couldn't run. i.e. Alluxio worker, fsadmin
   *                    shell, etc.
   * @return a string with the message
   */
  public static String getJobMasterHostNotConfiguredMessage(String serviceName) {
    return getHostNotConfiguredMessage(serviceName, "job master", PropertyKey.JOB_MASTER_HOSTNAME,
        PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES);
  }

  private static String getHostNotConfiguredMessage(String serviceName, String masterName,
      PropertyKey masterHostnameKey, PropertyKey embeddedJournalKey) {
    return ExceptionMessage.UNABLE_TO_DETERMINE_MASTER_HOSTNAME.getMessage(serviceName, masterName,
        masterHostnameKey.getName(), PropertyKey.ZOOKEEPER_ENABLED.getName(),
        PropertyKey.ZOOKEEPER_ADDRESS.getName(), embeddedJournalKey.getName());
  }

  /**
   * Checks that the given property key is a ratio from 0.0 and 1.0, throwing an exception if it is
   * not.
   *
   * @param conf the configuration for looking up the property key
   * @param key the property key
   * @return the property value
   */
  public static float checkRatio(AlluxioConfiguration conf, PropertyKey key) {
    float value = conf.getFloat(key);
    Preconditions.checkState(value <= 1.0, "Property %s must not exceed 1, but it is set to %s",
        key.getName(), value);
    Preconditions.checkState(value >= 0.0, "Property %s must be non-negative, but it is set to %s",
        key.getName(), value);
    return value;
  }

  /**
   * @param conf the configuration to use
   * @return whether the configuration describes how to find the master host, either through
   *         explicit configuration or through zookeeper
   */
  public static boolean masterHostConfigured(AlluxioConfiguration conf) {
    boolean usingZk = conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)
        && conf.isSet(PropertyKey.ZOOKEEPER_ADDRESS);
    return conf.isSet(PropertyKey.MASTER_HOSTNAME) || usingZk
        || getMasterRpcAddresses(conf).size() > 1;
  }

  /**
   * @param conf the configuration use
   * @return whether the configuration specifies to run in ha mode
   */
  public static boolean isHaMode(AlluxioConfiguration conf) {
    return conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED) || getMasterRpcAddresses(conf).size() > 1;
  }

  /**
   * Gets all configuration properties filtered by the specified scope.
   *
   * @param conf the configuration to use
   * @param scope the scope to filter by
   * @return the properties
   */
  public static List<ConfigProperty> getConfiguration(AlluxioConfiguration conf, Scope scope) {
    ConfigurationValueOptions useRawDisplayValue =
        ConfigurationValueOptions.defaults().useDisplayValue(true);

    List<ConfigProperty> configs = new ArrayList<>();
    List<PropertyKey> selectedKeys =
        conf.keySet().stream()
            .filter(key -> GrpcUtils.contains(key.getScope(), scope))
            .filter(key -> key.isValid(key.getName()))
            .collect(toList());

    for (PropertyKey key : selectedKeys) {
      ConfigProperty.Builder configProp = ConfigProperty.newBuilder().setName(key.getName())
          .setSource(conf.getSource(key).toString());
      if (conf.isSet(key)) {
        configProp.setValue(conf.get(key, useRawDisplayValue));
      }
      configs.add(configProp.build());
    }
    return configs;
  }

  /**
   * @param value the value or null (value is not set)
   * @return the value or "(no value set)" when the value is not set
   */
  public static String valueAsString(String value) {
    return value == null ? "(no value set)" : value;
  }

  /**
   * Returns an instance of {@link AlluxioConfiguration} with the defaults and values from
   * alluxio-site properties.
   *
   * @return the set of Alluxio properties loaded from the site-properties file
   */
  public static AlluxioProperties defaults() {
    if (sDefaultProperties == null) {
      synchronized (DEFAULT_PROPERTIES_LOCK) { // We don't want multiple threads to reload
        // properties at the same time.
        // Check if properties are still null so we don't reload a second time.
        if (sDefaultProperties == null) {
          reloadProperties();
        }
      }
    }
    return sDefaultProperties.copy();
  }

  /**
   * Reloads site properties from disk.
   */
  public static void reloadProperties() {
    synchronized (DEFAULT_PROPERTIES_LOCK) {
      // Step1: bootstrap the configuration. This is necessary because we need to resolve alluxio
      // .home (likely to be in system properties) to locate the conf dir to search for the site
      // property file.
      AlluxioProperties properties = new AlluxioProperties();
      InstancedConfiguration conf = new InstancedConfiguration(properties);
      // Can't directly pass System.getProperties() because it is not thread-safe
      // This can cause a ConcurrentModificationException when merging.
      Properties sysProps = new Properties();
      System.getProperties().stringPropertyNames()
          .forEach(key -> sysProps.setProperty(key, System.getProperty(key)));
      properties.merge(sysProps, Source.SYSTEM_PROPERTY);

      // Step2: Load site specific properties file if not in test mode. Note that we decide
      // whether in test mode by default properties and system properties (via getBoolean).
      if (conf.getBoolean(PropertyKey.TEST_MODE)) {
        conf.validate();
        sDefaultProperties = properties;
        return;
      }

      // we are not in test mode, load site properties
      String confPaths = conf.get(PropertyKey.SITE_CONF_DIR);
      String[] confPathList = confPaths.split(",");
      String sitePropertyFile = ConfigurationUtils
          .searchPropertiesFile(Constants.SITE_PROPERTIES, confPathList);
      Properties siteProps = null;
      if (sitePropertyFile != null) {
        siteProps = loadPropertiesFromFile(sitePropertyFile);
        sSourcePropertyFile = sitePropertyFile;
      } else {
        URL resource =
            ConfigurationUtils.class.getClassLoader().getResource(Constants.SITE_PROPERTIES);
        if (resource != null) {
          siteProps = loadPropertiesFromResource(resource);
          if (siteProps != null) {
            sSourcePropertyFile = resource.getPath();
          }
        }
      }
      properties.merge(siteProps, Source.siteProperty(sSourcePropertyFile));
      conf.validate();
      sDefaultProperties = properties;
    }
  }

  /**
   * Merges the current configuration properties with new properties. If a property exists
   * both in the new and current configuration, the one from the new configuration wins if
   * its priority is higher or equal than the existing one.
   *
   * @param conf the base configuration
   * @param properties the source {@link Properties} to be merged
   * @param source the source of the the properties (e.g., system property, default and etc)
   * @return a new configuration representing the merged properties
   */
  public static AlluxioConfiguration merge(AlluxioConfiguration conf, Map<?, ?> properties,
      Source source) {
    AlluxioProperties props = conf.copyProperties();
    props.merge(properties, source);
    return new InstancedConfiguration(props);
  }

  /**
   * Loads configuration from meta master in one RPC.
   *
   * @param address the meta master address
   * @param conf the existing configuration
   * @param ignoreClusterConf do not load cluster configuration related information
   * @param ignorePathConf do not load path configuration related information
   * @return the RPC response
   */
  public static GetConfigurationPResponse loadConfiguration(InetSocketAddress address,
      AlluxioConfiguration conf, boolean ignoreClusterConf, boolean ignorePathConf)
      throws AlluxioStatusException {
    GrpcChannel channel = null;
    try {
      LOG.debug("Alluxio client (version {}) is trying to load configuration from meta master {}",
          RuntimeConstants.VERSION, address);
      channel = GrpcChannelBuilder.newBuilder(GrpcServerAddress.create(address), conf)
          .setClientType("ConfigurationUtils").disableAuthentication().build();
      MetaMasterConfigurationServiceGrpc.MetaMasterConfigurationServiceBlockingStub client =
          MetaMasterConfigurationServiceGrpc.newBlockingStub(channel);
      GetConfigurationPResponse response = client.getConfiguration(
          GetConfigurationPOptions.newBuilder().setRawValue(true)
              .setIgnoreClusterConf(ignoreClusterConf).setIgnorePathConf(ignorePathConf).build());
      LOG.debug("Alluxio client has loaded configuration from meta master {}", address);
      return response;
    } catch (io.grpc.StatusRuntimeException e) {
      throw new UnavailableException(String.format(
          "Failed to handshake with master %s to load cluster default configuration values: %s",
          address, e.getMessage()), e);
    } catch (UnauthenticatedException e) {
      throw new RuntimeException(String.format(
          "Received authentication exception during boot-strap connect with host:%s", address),
          e);
    } finally {
      if (channel != null) {
        channel.shutdown();
      }
    }
  }

  /**
   * Filters and loads properties with a certain scope from the property list returned by grpc.
   * The given scope should only be {@link Scope#WORKER} or {@link Scope#CLIENT}.
   *
   * @param properties the property list returned by grpc
   * @param scope the scope to filter the received property list
   * @param logMessage a function with key and value as parameter and returns debug log message
   * @return the loaded properties
   */
  private static Properties filterAndLoadProperties(List<ConfigProperty> properties,
      Scope scope, BiFunction<PropertyKey, String, String> logMessage) {
    Properties props = new Properties();
    for (ConfigProperty property : properties) {
      String name = property.getName();
      // TODO(binfan): support propagating unsetting properties from master
      if (PropertyKey.isValid(name) && property.hasValue()) {
        PropertyKey key = PropertyKey.fromString(name);
        if (!GrpcUtils.contains(key.getScope(), scope)) {
          // Only propagate properties contains the target scope
          continue;
        }
        String value = property.getValue();
        props.put(key, value);
        LOG.debug(logMessage.apply(key, value));
      }
    }
    return props;
  }

  /**
   * Loads the cluster level configuration from the get configuration response,
   * filters out the configuration for certain scope, and merges it with the existing configuration.
   *
   * @param response the get configuration RPC response
   * @param conf the existing configuration
   * @param scope the target scope
   * @return the merged configuration
   */
  public static AlluxioConfiguration getClusterConf(GetConfigurationPResponse response,
      AlluxioConfiguration conf, Scope scope) {
    String clientVersion = conf.get(PropertyKey.VERSION);
    LOG.debug("Alluxio {} (version {}) is trying to load cluster level configurations",
        scope, clientVersion);
    List<alluxio.grpc.ConfigProperty> clusterConfig = response.getClusterConfigsList();
    Properties clusterProps = filterAndLoadProperties(clusterConfig, scope, (key, value) ->
        String.format("Loading property: %s (%s) -> %s", key, key.getScope(), value));
    // Check version.
    String clusterVersion = clusterProps.get(PropertyKey.VERSION).toString();
    if (!clientVersion.equals(clusterVersion)) {
      LOG.warn("Alluxio {} version ({}) does not match Alluxio cluster version ({})",
          scope, clientVersion, clusterVersion);
      clusterProps.remove(PropertyKey.VERSION);
    }
    // Merge conf returned by master as the cluster default into conf object
    AlluxioProperties props = conf.copyProperties();
    props.merge(clusterProps, Source.CLUSTER_DEFAULT);
    // Use the constructor to set cluster defaults as being loaded.
    InstancedConfiguration updatedConf = new InstancedConfiguration(props, true);
    updatedConf.validate();
    LOG.debug("Alluxio {} has loaded cluster level configurations", scope);
    return updatedConf;
  }

  /**
   * Loads the path level configuration from the get configuration response.
   *
   * Only client scope properties will be loaded.
   *
   * @param response the get configuration RPC response
   * @param clusterConf cluster level configuration
   * @return the loaded path level configuration
   */
  public static PathConfiguration getPathConf(GetConfigurationPResponse response,
      AlluxioConfiguration clusterConf) {
    String clientVersion = clusterConf.get(PropertyKey.VERSION);
    LOG.debug("Alluxio client (version {}) is trying to load path level configurations",
        clientVersion);
    Map<String, AlluxioConfiguration> pathConfs = new HashMap<>();
    response.getPathConfigsMap().forEach((path, conf) -> {
      Properties props = filterAndLoadProperties(conf.getPropertiesList(), Scope.CLIENT,
          (key, value) -> String.format("Loading property: %s (%s) -> %s for path %s",
              key, key.getScope(), value, path));
      AlluxioProperties properties = new AlluxioProperties();
      properties.merge(props, Source.PATH_DEFAULT);
      pathConfs.put(path, new InstancedConfiguration(properties, true));
    });
    LOG.debug("Alluxio client has loaded path level configurations");
    return PathConfiguration.create(pathConfs);
  }

  /**
   * @param conf the configuration
   * @return the alluxio scheme and authority determined by the configuration
   */
  public static String getSchemeAuthority(AlluxioConfiguration conf) {
    if (conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      return Constants.HEADER + "zk@" + conf.get(PropertyKey.ZOOKEEPER_ADDRESS);
    }
    List<InetSocketAddress> addresses = getMasterRpcAddresses(conf);

    if (addresses.size() > 1) {
      return Constants.HEADER + addresses.stream().map(InetSocketAddress::toString)
          .collect(Collectors.joining(","));
    }

    return Constants.HEADER + conf.get(PropertyKey.MASTER_HOSTNAME) + ":" + conf
        .get(PropertyKey.MASTER_RPC_PORT);
  }

  /**
   * Returns the input string as a list, splitting on a specified delimiter.
   *
   * @param value the value to split
   * @param delimiter the delimiter to split the values
   * @return the list of values for input string
   */
  public static List<String> parseAsList(String value, String delimiter) {
    return Lists.newArrayList(Splitter.on(delimiter).trimResults().omitEmptyStrings()
        .split(value));
  }

  /**
   * Reads a list of nodes from given file name ignoring comments and empty lines.
   * Can be used to read conf/workers or conf/masters.
   * @param fileName name of a file that contains the list of the nodes
   * @return list of the node names, null when file fails to read
   */
  @Nullable
  private static Set<String> readNodeList(String fileName, AlluxioConfiguration conf) {
    String confDir = conf.get(PropertyKey.CONF_DIR);
    return CommandUtils.readNodeList(confDir, fileName);
  }

  /**
   * Gets list of masters in conf directory.
   *
   * @param conf configuration
   * @return master hostnames
   */
  public static Set<String> getMasterHostnames(AlluxioConfiguration conf) {
    return readNodeList(MASTERS, conf);
  }

  /**
   * Gets list of workers in conf directory.
   *
   * @param conf configuration
   * @return workers hostnames
   */
  public static Set<String> getWorkerHostnames(AlluxioConfiguration conf) {
    return readNodeList(WORKERS, conf);
  }

  /**
   * Gets list of masters/workers in conf directory.
   *
   * @param conf configuration
   * @return server hostnames
   */
  public static Set<String> getServerHostnames(AlluxioConfiguration conf) {
    return Sets.union(getMasterHostnames(conf), getWorkerHostnames(conf));
  }
}
