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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.options.OpenOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.annotation.Nullable;

public class MockObjectUnderFileSystem extends ObjectUnderFileSystem {
  /**
   * Constructs an {@link ObjectUnderFileSystem}.
   *
   * @param uri     the {@link AlluxioURI} used to create this ufs
   * @param ufsConf UFS configuration
   */
  protected MockObjectUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    super(uri, ufsConf);
  }

  @Override
  public boolean createEmptyObject(String key) {
    return false;
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return null;
  }

  @Override
  protected boolean copyObject(String src, String dst) throws IOException {
    return false;
  }

  @Override
  protected boolean deleteObject(String key) throws IOException {
    return false;
  }

  @Override
  protected ObjectPermissions getPermissions() {
    return null;
  }

  @Nullable
  @Override
  protected ObjectStatus getObjectStatus(String key) throws IOException {
    return null;
  }

  @Override
  protected String getFolderSuffix() {
    return null;
  }

  @Nullable
  @Override
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {
    return null;
  }

  @Override
  protected String getRootKey() {
    return null;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options, RetryPolicy retryPolicy)
      throws IOException {
    return null;
  }

  @Override
  public String getUnderFSType() {
    return null;
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    //do nothing
  }

  @Override
  public void setOwner(String path, String owner, String group) throws IOException {
    //do nothing
  }
}
