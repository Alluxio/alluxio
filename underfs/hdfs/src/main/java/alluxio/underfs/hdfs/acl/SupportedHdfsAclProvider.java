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
import alluxio.underfs.hdfs.HdfsAclProvider;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  @Override
  public void setAcl(FileSystem hdfs, String path, AccessControlList acl) throws IOException {
    // convert AccessControlList into hdfsAcl
    List<AclEntry> aclSpecs = new ArrayList<>();
    for (alluxio.security.authorization.AclEntry entry : acl.getEntries()) {
      aclSpecs.add(getHdfsAclEntry(entry));
    }
    // set hdfsAcl;
    hdfs.setAcl(new Path(path), aclSpecs);
    // set owner and owning group
    hdfs.setOwner(new Path(path), acl.getOwningUser(), acl.getOwningGroup());
  }

  private AclEntry getHdfsAclEntry(alluxio.security.authorization.AclEntry entry)
      throws IOException {
    AclEntry.Builder builder = new AclEntry.Builder();
    builder.setName(entry.getSubject());
    builder.setScope(entry.isDefault() ? AclEntryScope.DEFAULT : AclEntryScope.ACCESS);
    builder.setType(getHdfsAclEntryType(entry));
    FsAction permission = FsAction.getFsAction(entry.getActions().toCliString());
    builder.setPermission(permission);
    return builder.build();
  }

  private AclEntryType getHdfsAclEntryType(alluxio.security.authorization.AclEntry aclEntry)
      throws IOException {
    switch (aclEntry.getType()) {
      case OWNING_USER:
      case NAMED_USER:
        return AclEntryType.USER;
      case OWNING_GROUP:
      case NAMED_GROUP:
        return AclEntryType.GROUP;
      case MASK:
        return AclEntryType.MASK;
      case OTHER:
        return AclEntryType.OTHER;
      default:
        throw new IOException("Unknown Alluxio ACL entry type: " + aclEntry.getType());
    }
  }

  private alluxio.security.authorization.AclEntryType getAclEntryType(AclEntry entry)
      throws IOException {
    switch (entry.getType()) {
      case USER:
        return entry.getName().isEmpty() ? alluxio.security.authorization.AclEntryType.OWNING_USER
            : alluxio.security.authorization.AclEntryType.NAMED_USER;
      case GROUP:
        return entry.getName().isEmpty() ? alluxio.security.authorization.AclEntryType.OWNING_GROUP
            : alluxio.security.authorization.AclEntryType.NAMED_GROUP;
      case MASK:
        return alluxio.security.authorization.AclEntryType.MASK;
      case OTHER:
        return alluxio.security.authorization.AclEntryType.OTHER;
      default:
        throw new IOException("Unknown HDFS ACL entry type: " + entry.getType());
    }
  }
}
