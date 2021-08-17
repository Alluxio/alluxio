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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.master.audit.AuditContext;
import alluxio.master.file.meta.Inode;
import alluxio.security.authentication.AuthType;
import alluxio.security.authorization.Mode;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An audit context for file system master.
 */
@NotThreadSafe
public final class FileSystemMasterAuditContext implements AuditContext {
  private final AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;
  private boolean mAllowed;
  private boolean mSucceeded;
  private String mCommand;
  private AlluxioURI mSrcPath;
  private AlluxioURI mDstPath;
  private String mUgi;
  private AuthType mAuthType;
  private String mIp;
  private Inode mSrcInode;
  private long mCreationTimeNs;
  private long mExecutionTimeNs;

  @Override
  public FileSystemMasterAuditContext setAllowed(boolean allowed) {
    mAllowed = allowed;
    return this;
  }

  @Override
  public FileSystemMasterAuditContext setSucceeded(boolean succeeded) {
    mSucceeded = succeeded;
    return this;
  }

  /**
   * Sets mCommand field.
   *
   * @param command the command associated with this {@link alluxio.master.Master}
   * @return this {@link AuditContext} instance
   */
  public FileSystemMasterAuditContext setCommand(String command) {
    mCommand = command;
    return this;
  }

  /**
   * Sets mSrcPath field.
   *
   * @param srcPath the source path of the command
   * @return this {@link AuditContext} instance
   */
  public FileSystemMasterAuditContext setSrcPath(AlluxioURI srcPath) {
    mSrcPath = srcPath;
    return this;
  }

  /**
   * Sets mDstPath field.
   *
   * @param dstPath the destination path of the command
   * @return this {@link AuditContext} instance
   */
  public FileSystemMasterAuditContext setDstPath(AlluxioURI dstPath) {
    mDstPath = dstPath;
    return this;
  }

  /**
   * Sets mUgi field.
   *
   * @param ugi the client user name of the authenticated client user of this thread
   * @return this {@link AuditContext} instance
   */
  public FileSystemMasterAuditContext setUgi(String ugi) {
    mUgi = ugi;
    return this;
  }

  /**
   * Sets mAuthType field.
   *
   * @param authType the authentication type
   * @return this {@link AuditContext} instance
   */
  public FileSystemMasterAuditContext setAuthType(AuthType authType) {
    mAuthType = authType;
    return this;
  }

  /**
   * Sets mIp field.
   *
   * @param ip the IP of the client
   * @return this {@link AuditContext} instance
   */
  public FileSystemMasterAuditContext setIp(String ip) {
    mIp = ip;
    return this;
  }

  /**
   * Sets mSrcInode field.
   *
   * @param srcInode the source inode of this operation
   * @return this {@link AuditContext} instance
   */
  public FileSystemMasterAuditContext setSrcInode(Inode srcInode) {
    mSrcInode = srcInode;
    return this;
  }

  /**
   * Sets mCreationTimeNs field.
   *
   * @param creationTimeNs the System.nanoTime() when this operation create,
   *                     it only can be used to compute operation mExecutionTime
   * @return this {@link AuditContext} instance
   */
  public FileSystemMasterAuditContext setCreationTimeNs(long creationTimeNs) {
    mCreationTimeNs = creationTimeNs;
    return this;
  }

  /**
   * Constructor of {@link FileSystemMasterAuditContext}.
   *
   * @param asyncAuditLogWriter
   */
  protected FileSystemMasterAuditContext(AsyncUserAccessAuditLogWriter asyncAuditLogWriter) {
    mAsyncAuditLogWriter = asyncAuditLogWriter;
    mAllowed = true;
  }

  @Override
  public void close() {
    if (mAsyncAuditLogWriter == null) {
      return;
    }
    mExecutionTimeNs = System.nanoTime() - mCreationTimeNs;
    mAsyncAuditLogWriter.append(this);
  }

  @Override
  public String toString() {
    if (mSrcInode != null) {
      short mode = mSrcInode.getMode();
      return String.format(
          "succeeded=%b\tallowed=%b\tugi=%s (AUTH=%s)\tip=%s\tcmd=%s\tsrc=%s\tdst=%s\t"
              + "perm=%s:%s:%s%s%s\texecutionTimeUs=%d",
          mSucceeded, mAllowed, mUgi, mAuthType, mIp, mCommand, mSrcPath, mDstPath,
          mSrcInode.getOwner(), mSrcInode.getGroup(),
          Mode.extractOwnerBits(mode), Mode.extractGroupBits(mode), Mode.extractOtherBits(mode),
          mExecutionTimeNs / 1000);
    } else {
      return String.format(
          "succeeded=%b\tallowed=%b\tugi=%s (AUTH=%s)\tip=%s\tcmd=%s\tsrc=%s\tdst=%s\t"
              + "perm=null\texecutionTimeUs=%d",
          mSucceeded, mAllowed, mUgi, mAuthType, mIp, mCommand, mSrcPath, mDstPath,
          mExecutionTimeNs / 1000);
    }
  }
}
