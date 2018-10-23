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

package alluxio.underfs.hdfs;

import alluxio.collections.Pair;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Interface for providing HDFS ACLs.
 */
public interface HdfsAclProvider {
  /**
   * Returns the {@link AccessControlList} from an hdfs path.
   *
   * @param hdfs the HDFS client
   * @param path the path to retrieve the ACL for
   * @return a Pair object containing the {@link AccessControlList} representation and
   *         the  {@link DefaultAccessControlList} representation or null if ACL is
   *         unsupported/disabled
   * @throws IOException if ACL is supported but cannot be retrieved
   */
  @Nullable
  Pair<AccessControlList, DefaultAccessControlList> getAcl(FileSystem hdfs, String path)
      throws IOException;

  /**
   * Sets the ACL and default ACL for an hdfs path using ACL entries.
   *
   * @param hdfs the HDFS client
   * @param path the path to set the ACL entries for
   * @param aclEntries list of AclEntries, could be a representation of {@link AccessControlList}
   *                   or {@link DefaultAccessControlList}
   * @throws IOException if ACL can not be set
   */
  void setAclEntries(FileSystem hdfs, String path, List<AclEntry> aclEntries) throws IOException;
}
