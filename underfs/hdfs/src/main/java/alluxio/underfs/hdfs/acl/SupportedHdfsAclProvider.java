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

package alluxio.underfs.hdfs.acl;

import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclEntryType;
import alluxio.underfs.hdfs.HdfsAclProvider;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides the HDFS ACLs. This is only supported in Hadoop versions 2.4 and greater.
 */
@ThreadSafe
public class SupportedHdfsAclProvider implements HdfsAclProvider {
  private static final Logger LOG = LoggerFactory.getLogger(SupportedHdfsAclProvider.class);

  @Override
  public AccessControlList getAcl(FileSystem hdfs, String path) throws IOException {
    AclStatus hdfsAcl;
    try {
      hdfsAcl = hdfs.getAclStatus(new Path(path));
    } catch (AclException e) {
      // When dfs.namenode.acls.enabled is false, getAclStatus throws AclException.
      return null;
    }
    AccessControlList acl = new AccessControlList();
    acl.setOwningUser(hdfsAcl.getOwner());
    acl.setOwningGroup(hdfsAcl.getGroup());
    for (AclEntry entry : hdfsAcl.getEntries()) {
      // TODO(chen): handle the case where the entry is for default ACL in a directory.
      alluxio.security.authorization.AclEntry.Builder builder =
          new alluxio.security.authorization.AclEntry.Builder();
      builder.setType(getAclEntryType(entry));
      builder.setSubject(entry.getName());
      FsAction permission = entry.getPermission();
      if (permission.implies(FsAction.READ)) {
        builder.addAction(AclAction.READ);
      } else if (permission.implies(FsAction.WRITE)) {
        builder.addAction(AclAction.WRITE);
      } else if (permission.implies(FsAction.EXECUTE)) {
        builder.addAction(AclAction.EXECUTE);
      }
      acl.setEntry(builder.build());
    }
    return acl;
  }

  private AclEntryType getAclEntryType(AclEntry entry) throws IOException {
    switch (entry.getType()) {
      case USER:
        return entry.getName().isEmpty() ? AclEntryType.OWNING_USER : AclEntryType.NAMED_USER;
      case GROUP:
        return entry.getName().isEmpty() ? AclEntryType.OWNING_GROUP : AclEntryType.NAMED_GROUP;
      case MASK:
        return AclEntryType.MASK;
      case OTHER:
        return AclEntryType.OTHER;
      default:
        throw new IOException("Unknown ACL entry type: " + entry.getType());
    }
  }
}
