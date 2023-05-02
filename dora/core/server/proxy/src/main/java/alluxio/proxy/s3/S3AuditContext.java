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

package alluxio.proxy.s3;

import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.master.audit.AuditContext;

/**
 * An audit context for s3 rest service.
 */
public class S3AuditContext implements AuditContext {
  private final AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;
  private boolean mAllowed;
  private boolean mSucceeded;
  private String mCommand;
  private String mUgi;
  private String mIp;
  private String mBucket;
  private String mObject;
  private long mCreationTimeNs;
  private long mExecutionTimeNs;

  /**
   * Constructor of {@link S3AuditContext}.
   *
   * @param asyncAuditLogWriter
   */
  public S3AuditContext(AsyncUserAccessAuditLogWriter asyncAuditLogWriter) {
    mAsyncAuditLogWriter = asyncAuditLogWriter;
    mAllowed = true;
  }

  /**
   * Sets mUgi field.
   *
   * @param ugi the client user name of the client user of this request
   * @return this {@link AuditContext} instance
   */
  public S3AuditContext setUgi(String ugi) {
    mUgi = ugi;
    return this;
  }

  /**
   * Sets mCommand field.
   *
   * @param command the command associated with S3 rest service
   * @return this {@link AuditContext} instance
   */
  public S3AuditContext setCommand(String command) {
    mCommand = command;
    return this;
  }

  /**
   * Sets mIp field.
   *
   * @param ip the IP of the client
   * @return this {@link AuditContext} instance
   */
  public S3AuditContext setIp(String ip) {
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
  public S3AuditContext setCreationTimeNs(long creationTimeNs) {
    mCreationTimeNs = creationTimeNs;
    return this;
  }

  /**
   * Sets mBucket field.
   * @param bucket the bucket name
   * @return this {@link AuditContext} instance
   */
  public S3AuditContext setBucket(String bucket) {
    mBucket = bucket;
    return this;
  }

  /**
   * Sets mObject field.
   * @param object the object name
   * @return this {@link AuditContext} instance
   */
  public S3AuditContext setObject(String object) {
    mObject = object;
    return this;
  }

  @Override
  public S3AuditContext setAllowed(boolean allowed) {
    mAllowed = allowed;
    return this;
  }

  @Override
  public S3AuditContext setSucceeded(boolean succeeded) {
    mSucceeded = succeeded;
    return this;
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
    return String.format(
      "succeeded=%b\tallowed=%b\tugi=%s\tip=%s\tcmd=%s\t"
      + "bucket=%s\tobject=%s\texecutionTimeUs=%d",
      mSucceeded, mAllowed, mUgi, mIp, mCommand, mBucket, mObject, mExecutionTimeNs / 1000);
  }
}

