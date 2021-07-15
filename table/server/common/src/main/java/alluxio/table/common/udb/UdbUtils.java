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

package alluxio.table.common.udb;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.MountPOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Udb related utils.
 */
public class UdbUtils {
  private UdbUtils() {}

  private static final Logger LOG = LoggerFactory.getLogger(UdbUtils.class);

  /**
   * Mount ufs path to alluxio path.
   *
   * @param tableName Table name
   * @param ufsUri the uri of ufs
   * @param tableUri the alluxio uri for table
   * @param udbContext udb context
   * @param udbConfiguration Udb configurations
   * @return table uri
   * @throws IOException
   * @throws AlluxioException
   */
  // Maybe rename to mountTable?
  public static String mountAlluxioPath(String tableName, AlluxioURI ufsUri, AlluxioURI tableUri,
      UdbContext udbContext, UdbConfiguration udbConfiguration)
      throws IOException, AlluxioException {
    String path = mount(ufsUri, tableUri, udbContext, udbConfiguration);
    LOG.info("mounted table {} location {} to Alluxio location {}", tableName, ufsUri, tableUri);
    return path;
  }

  /**
   * Mount a fragment of a UDB.
   *
   * @param dbName name of the UDB
   * @param ufsUri UFS URI of the fragment
   * @param fragmentUri URI of the mount point in Alluxio
   * @param udbContext udb context
   * @param udbConfiguration udb configuration
   * @return fragment path in Alluxio
   */
  public static String mountFragment(String dbName, AlluxioURI ufsUri, AlluxioURI fragmentUri,
      UdbContext udbContext, UdbConfiguration udbConfiguration)
      throws IOException, AlluxioException {
    String path = mount(ufsUri, fragmentUri, udbContext, udbConfiguration);
    LOG.info("mounted database {} fragment {} to Alluxio location {}", dbName, ufsUri, fragmentUri);
    return path;
  }

  private static String mount(AlluxioURI ufsUri, AlluxioURI alluxioUri, UdbContext udbContext,
      UdbConfiguration udbConfiguration)
      throws IOException, AlluxioException {
    if (Objects.equals(ufsUri.getScheme(), Constants.SCHEME)) {
      // already an alluxio uri, return the alluxio uri
      return ufsUri.toString();
    }
    try {
      alluxioUri = udbContext.getFileSystem().reverseResolve(ufsUri);
      LOG.debug("Trying to mount ufs location {}, but it is already mounted at location {}",
          ufsUri, alluxioUri);
      return alluxioUri.getPath();
    } catch (InvalidPathException e) {
      // ufs path not mounted, continue
    }
    // make sure the parent exists
    udbContext.getFileSystem().createDirectory(alluxioUri.getParent(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
    Map<String, String> mountOptionMap = udbConfiguration.getMountOption(
        String.format("%s://%s/", ufsUri.getScheme(), ufsUri.getAuthority().toString()));
    MountPOptions.Builder option = MountPOptions.newBuilder();
    for (Map.Entry<String, String> entry : mountOptionMap.entrySet()) {
      if (entry.getKey().equals(UdbConfiguration.READ_ONLY_OPTION)) {
        option.setReadOnly(Boolean.parseBoolean(entry.getValue()));
      } else if (entry.getKey().equals(UdbConfiguration.SHARED_OPTION)) {
        option.setShared(Boolean.parseBoolean(entry.getValue()));
      } else {
        option.putProperties(entry.getKey(), entry.getValue());
      }
    }
    udbContext.getFileSystem().mount(alluxioUri, ufsUri, option.build());

    LOG.info("mounted ufs location {} to Alluxio location {} with mountOption {}",
        ufsUri, alluxioUri, option.build());
    return alluxioUri.getPath();
  }
}
