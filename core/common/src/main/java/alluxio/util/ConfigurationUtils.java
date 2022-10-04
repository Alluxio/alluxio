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

import alluxio.Constants;
import alluxio.cli.CommandUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Utilities for working with Alluxio configurations.
 */
public final class ConfigurationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtils.class);

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
      return parseInetSocketAddresses(conf.getList(property));
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
      return parseInetSocketAddresses(conf.getList(jobMasterProperty));
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
      return parseInetSocketAddresses(conf.getList(PropertyKey.MASTER_RPC_ADDRESSES));
    }

    // Fall back on server-side journal configuration.
    int rpcPort = NetworkAddressUtils.getPort(NetworkAddressUtils.ServiceType.MASTER_RPC, conf);
    return overridePort(getEmbeddedJournalAddresses(conf, ServiceType.MASTER_RAFT), rpcPort);
  }

  /**
   * @param conf the configuration to use
   * @return the list of addresses of the cross cluster configuration service
   */
  public static List<InetSocketAddress> getCrossClusterConfigAddresses(AlluxioConfiguration conf) {
    if (conf.isSet(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES)) {
      return parseInetSocketAddresses(conf.getList(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES));
    }
    throw new IllegalStateException(String.format(
        "Property key %s must be set when using cross cluster",
        PropertyKey.Name.MASTER_CROSS_CLUSTER_RPC_ADDRESSES));
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
          conf.getList(PropertyKey.JOB_MASTER_RPC_ADDRESSES));
    }

    int jobRpcPort =
        NetworkAddressUtils.getPort(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC, conf);
    // Fall back on explicitly configured regular master rpc addresses.
    if (conf.isSet(PropertyKey.MASTER_RPC_ADDRESSES)) {
      List<InetSocketAddress> addrs =
          parseInetSocketAddresses(conf.getList(PropertyKey.MASTER_RPC_ADDRESSES));
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
  public static List<InetSocketAddress> parseInetSocketAddresses(List<String> addresses) {
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
    double value = conf.getDouble(key);
    Preconditions.checkState(value <= 1.0, "Property %s must not exceed 1, but it is set to %s",
        key.getName(), value);
    Preconditions.checkState(value >= 0.0, "Property %s must be non-negative, but it is set to %s",
        key.getName(), value);
    return (float) value;
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
   * @param value the value or null (value is not set)
   * @return the value or "(no value set)" when the value is not set
   */
  public static String valueAsString(String value) {
    return value == null ? "(no value set)" : value;
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
    String confDir = conf.getString(PropertyKey.CONF_DIR);
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
