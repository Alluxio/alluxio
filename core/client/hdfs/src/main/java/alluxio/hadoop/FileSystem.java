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

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.exception.PreconditionMessage;
import alluxio.uri.Authority;
import alluxio.uri.EmbeddedLogicalAuthority;
import alluxio.uri.MultiMasterAuthority;
import alluxio.uri.SingleMasterAuthority;
import alluxio.uri.UnknownAuthority;
import alluxio.uri.ZookeeperAuthority;
import alluxio.uri.ZookeeperLogicalAuthority;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An Alluxio client API compatible with Apache Hadoop {@link org.apache.hadoop.fs.FileSystem}
 * interface. Any program working with Hadoop HDFS can work with Alluxio transparently. Note that
 * the performance of using this API may not be as efficient as the performance of using the Alluxio
 * native API defined in {@link alluxio.client.file.FileSystem}, which this API is built on top of.
 */
@NotThreadSafe
public final class FileSystem extends AbstractFileSystem {
  /**
   * Constructs a new {@link FileSystem}.
   */
  public FileSystem() {
    super();
  }

  /**
   * Constructs a new {@link FileSystem} instance with a
   * specified {@link alluxio.client.file.FileSystem} handler for tests.
   *
   * @param fileSystem handler to file system
   */
  public FileSystem(alluxio.client.file.FileSystem fileSystem) {
    super(fileSystem);
  }

  @Override
  public String getScheme() {
    return Constants.SCHEME;
  }

  @Override
  protected boolean isZookeeperMode() {
    return mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
  }

  @Override
  protected Map<String, Object> getConfigurationFromUri(URI uri, Configuration conf) {
    AlluxioURI alluxioUri = new AlluxioURI(uri.toString());
    Map<String, Object> alluxioConfProperties = new HashMap<>();

    if (alluxioUri.getAuthority() instanceof ZookeeperAuthority) {
      ZookeeperAuthority authority = (ZookeeperAuthority) alluxioUri.getAuthority();
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), true);
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(),
              authority.getZookeeperAddress());
    } else if (alluxioUri.getAuthority() instanceof SingleMasterAuthority) {
      SingleMasterAuthority authority = (SingleMasterAuthority) alluxioUri.getAuthority();
      alluxioConfProperties.put(PropertyKey.MASTER_HOSTNAME.getName(), authority.getHost());
      alluxioConfProperties.put(PropertyKey.MASTER_RPC_PORT.getName(), authority.getPort());
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), false);
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), null);
      // Unset the embedded journal related configuration
      // to support alluxio URI has the highest priority
      alluxioConfProperties.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES.getName(), null);
      alluxioConfProperties.put(PropertyKey.MASTER_RPC_ADDRESSES.getName(), null);
    } else if (alluxioUri.getAuthority() instanceof MultiMasterAuthority) {
      MultiMasterAuthority authority = (MultiMasterAuthority) alluxioUri.getAuthority();
      alluxioConfProperties.put(PropertyKey.MASTER_RPC_ADDRESSES.getName(),
              authority.getMasterAddresses());
      // Unset the zookeeper configuration to support alluxio URI has the highest priority
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), false);
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), null);
    } else if (alluxioUri.getAuthority() instanceof EmbeddedLogicalAuthority) {
      EmbeddedLogicalAuthority authority = (EmbeddedLogicalAuthority) alluxioUri.getAuthority();
      String masterNamesConfKey = PropertyKey.Template.MASTER_LOGICAL_NAMESERVICES
          .format(authority.getLogicalName()).getName();
      String[] masterNames = conf.getTrimmedStrings(masterNamesConfKey);
      Preconditions.checkArgument(masterNames.length != 0,
          "Invalid uri. You must set %s to use the logical name ", masterNamesConfKey);

      StringJoiner masterRpcAddress = new StringJoiner(",");
      for (String masterName : masterNames) {
        String name = PropertyKey.Template.MASTER_LOGICAL_RPC_ADDRESS
            .format(authority.getLogicalName(), masterName).getName();
        String address = conf.get(name);
        Preconditions.checkArgument(address != null, "You need to set %s", name);
        masterRpcAddress.add(address);
      }

      alluxioConfProperties.put(PropertyKey.MASTER_RPC_ADDRESSES.getName(),
          masterRpcAddress.toString());
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), false);
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), null);
    } else if (alluxioUri.getAuthority() instanceof ZookeeperLogicalAuthority) {
      ZookeeperLogicalAuthority authority = (ZookeeperLogicalAuthority) alluxioUri.getAuthority();
      String zkNodesConfKey = PropertyKey.Template.MASTER_LOGICAL_ZOOKEEPER_NAMESERVICES
          .format(authority.getLogicalName()).getName();
      String[] zkNodeNames = conf.getTrimmedStrings(zkNodesConfKey);
      Preconditions.checkArgument(zkNodeNames.length != 0,
          "Invalid uri. You must set %s to use the logical name", zkNodesConfKey);

      StringJoiner zkAddress = new StringJoiner(",");
      for (String zkName : zkNodeNames) {
        String name = PropertyKey.Template.MASTER_LOGICAL_ZOOKEEPER_ADDRESS
            .format(authority.getLogicalName(), zkName).getName();
        String address = conf.get(name);
        Preconditions.checkArgument(address != null, "You need to set %s", name);
        zkAddress.add(address);
      }

      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), true);
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), zkAddress.toString());
    }
    return alluxioConfProperties;
  }

  @Override
  protected void validateFsUri(URI fsUri) throws IOException, IllegalArgumentException {
    Preconditions.checkArgument(fsUri.getScheme().equals(getScheme()),
            PreconditionMessage.URI_SCHEME_MISMATCH.toString(), fsUri.getScheme(), getScheme());

    Authority auth = Authority.fromString(fsUri.getAuthority());
    if (auth instanceof UnknownAuthority) {
      throw new IOException(String.format("Authority \"%s\" is unknown. The client can not be "
              + "configured with the authority from %s", auth, fsUri));
    }
  }

  @Override
  protected String getFsScheme(URI fsUri) {
    return getScheme();
  }

  @Override
  protected AlluxioURI getAlluxioPath(Path path) {
    return new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
  }

  @Override
  protected Path getFsPath(String fsUriHeader, URIStatus fileStatus) {
    return new Path(fsUriHeader + fileStatus.getPath());
  }
}
