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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetConfigurationPOptions;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
    return StreamUtils.map(addr -> new InetSocketAddress(addr.getHostString(), port), addrs);
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
        ConfigurationValueOptions.defaults().useDisplayValue(true).useRawValue(true);

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
      properties.merge(System.getProperties(), Source.SYSTEM_PROPERTY);

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
   * Loads cluster default values from the meta master.
   *
   * @param address the master address
   * @param conf configuration to use
   * @return a configuration object containing the original configuration merged with cluster
   *         defaults, or the original object if the cluster defaults have already been loaded
   */
  public static AlluxioConfiguration loadClusterDefaults(InetSocketAddress address,
      AlluxioConfiguration conf)
      throws AlluxioStatusException {
    if (!conf.getBoolean(PropertyKey.USER_CONF_CLUSTER_DEFAULT_ENABLED)
        || conf.clusterDefaultsLoaded()) {
      return conf;
    }
    LOG.info("Alluxio client (version {}) is trying to bootstrap-connect with {}",
        RuntimeConstants.VERSION, address);

    GrpcChannel channel = null;
    List<alluxio.grpc.ConfigProperty> clusterConfig;

    try {
      channel = GrpcChannelBuilder.newBuilder(new GrpcServerAddress(address), conf)
          .disableAuthentication().build();
      MetaMasterConfigurationServiceGrpc.MetaMasterConfigurationServiceBlockingStub client =
          MetaMasterConfigurationServiceGrpc.newBlockingStub(channel);
      clusterConfig =
          client.getConfiguration(GetConfigurationPOptions.newBuilder().setRawValue(true).build())
              .getConfigsList();
    } catch (io.grpc.StatusRuntimeException e) {
      AlluxioStatusException ase = AlluxioStatusException.fromStatusRuntimeException(e);
      LOG.warn("Failed to handshake with master {} : {}", address, ase.getMessage());
      throw new UnavailableException(String.format(
          "Failed to handshake with master %s to load cluster default configuration values",
          address), e);
    } catch (UnauthenticatedException e) {
      throw new RuntimeException(String.format(
          "Received authentication exception during boot-strap connect with host:%s", address),
          e);
    } finally {
      if (channel != null) {
        channel.shutdown();
      }
    }

    // merge conf returned by master as the cluster default into conf object
    Properties clusterProps = new Properties();
    for (ConfigProperty property : clusterConfig) {
      String name = property.getName();
      // TODO(binfan): support propagating unsetting properties from master
      if (PropertyKey.isValid(name) && property.hasValue()) {
        PropertyKey key = PropertyKey.fromString(name);
        if (!GrpcUtils.contains(key.getScope(), Scope.CLIENT)) {
          // Only propagate client properties.
          continue;
        }
        String value = property.getValue();
        clusterProps.put(key, value);
        LOG.debug("Loading cluster default: {} ({}) -> {}", key, key.getScope(), value);
      }
    }

    String clientVersion = conf.get(PropertyKey.VERSION);
    String clusterVersion = clusterProps.get(PropertyKey.VERSION).toString();
    if (!clientVersion.equals(clusterVersion)) {
      LOG.warn("Alluxio client version ({}) does not match Alluxio cluster version ({})",
          clientVersion, clusterVersion);
      clusterProps.remove(PropertyKey.VERSION);
    }
    AlluxioProperties props = conf.copyProperties();
    props.merge(clusterProps, Source.CLUSTER_DEFAULT);
    // Use the constructor to set cluster defaults as being laoded.
    InstancedConfiguration updatedConf = new InstancedConfiguration(props, true);
    updatedConf.validate();
    LOG.info("Alluxio client has bootstrap-connected with {}", address);
    return updatedConf;
  }
}
