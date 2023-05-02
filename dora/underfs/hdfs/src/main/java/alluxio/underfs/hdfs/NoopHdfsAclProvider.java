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

/**
 * The noop HDFS ACL provider.
 */
public class NoopHdfsAclProvider implements HdfsAclProvider {
  @Override
  public Pair<AccessControlList, DefaultAccessControlList> getAcl(FileSystem hdfs, String path)
      throws IOException {
    return new Pair<>(null, null);
  }

  @Override
  public void setAclEntries(FileSystem hdfs, String path, List<AclEntry> aclEntries)
      throws IOException {
    // Noop for setAclEntries
  }
}
