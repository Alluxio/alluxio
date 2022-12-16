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

package alluxio.master.job;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.master.audit.AuditContext;
import alluxio.security.authentication.AuthType;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An audit context for job master.
 */
@NotThreadSafe
public class JobMasterAuditContext implements AuditContext {
  private final AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;
  private boolean mAllowed;
  private boolean mSucceeded;
  private String mCommand;
  private String mUgi;
  private AuthType mAuthType;
  private String mIp;
  private long mJobId;
  private String mJobName;
  private long mCreationTimeNs;
  private long mExecutionTimeNs;
  private String mClientVersion;
  private String mClientRevision;

  @Override
  public JobMasterAuditContext setAllowed(boolean allowed) {
    mAllowed = allowed;
    return this;
  }

  @Override
  public JobMasterAuditContext setSucceeded(boolean succeeded) {
    mSucceeded = succeeded;
    return this;
  }

  /**
   * Sets mCommand field.
   *
   * @param command the command associated with this {@link alluxio.master.Master}
   * @return this {@link AuditContext} instance
   */
  public JobMasterAuditContext setCommand(String command) {
    mCommand = command;
    return this;
  }

  /**
   * Sets mUgi field.
   *
   * @param ugi the client user name of the authenticated client user of this thread
   * @return this {@link AuditContext} instance
   */
  public JobMasterAuditContext setUgi(String ugi) {
    mUgi = ugi;
    return this;
  }

  /**
   * Sets mAuthType field.
   *
   * @param authType the authentication type
   * @return this {@link AuditContext} instance
   */
  public JobMasterAuditContext setAuthType(AuthType authType) {
    mAuthType = authType;
    return this;
  }

  /**
   * Sets mIp field.
   *
   * @param ip the IP of the client
   * @return this {@link AuditContext} instance
   */
  public JobMasterAuditContext setIp(String ip) {
    mIp = ip;
    return this;
  }

  /**
   * Sets mCreationTimeNs field.
   *
   * @param creationTimeNs the System.nanoTime() when this operation create,
   *                     it only can be used to compute operation mExecutionTime
   * @return this {@link AuditContext} instance
   */
  public JobMasterAuditContext setCreationTimeNs(long creationTimeNs) {
    mCreationTimeNs = creationTimeNs;
    return this;
  }

  /**
   * Sets mCreationTimeNs field.
   *
   * @param jobId the job id
   * @return this {@link AuditContext} instance
   */
  public JobMasterAuditContext setJobId(long jobId) {
    mJobId = jobId;
    return this;
  }

  /**
   * Sets mJobName field.
   *
   * @param jobName the job name
   * @return this {@link AuditContext} instance
   */
  public JobMasterAuditContext setJobName(String jobName) {
    mJobName = jobName;
    return this;
  }

  /**
   * Sets client version.
   *
   * @param clientVersion the client version
   * @return this {@link AuditContext} instance
   */
  public JobMasterAuditContext setClientVersion(String clientVersion) {
    mClientVersion = clientVersion;
    return this;
  }

  /**
   * set client revision.
   * @param revision client revision
   * @return this {@link AuditContext} instance
   */
  public JobMasterAuditContext setClientRevision(String revision) {
    mClientRevision = revision;
    return this;
  }

  /**
   * Constructor of {@link JobMasterAuditContext}.
   *
   * @param asyncAuditLogWriter async audit log writer
   */
  protected JobMasterAuditContext(AsyncUserAccessAuditLogWriter asyncAuditLogWriter) {
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
    StringBuilder auditLog = new StringBuilder();
    auditLog.append(String.format(
        "succeeded=%b\tallowed=%b\tugi=%s (AUTH=%s)\tip=%s\tcmd=%s\tjobId=%d\tjobName=%s\t"
            + "perm=null\texecutionTimeUs=%d",
        mSucceeded, mAllowed, mUgi, mAuthType, mIp, mCommand, mJobId, mJobName,
        mExecutionTimeNs / 1000));
    if (Configuration.global().getBoolean(PropertyKey.USER_CLIENT_REPORT_VERSION)) {
      auditLog.append(
          String.format("\tclientVersion=%s\tclientRevision=%s", mClientVersion, mClientRevision));
    }
    return auditLog.toString();
  }
}
