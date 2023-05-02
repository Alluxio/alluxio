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

package alluxio.master.audit;

import java.io.Closeable;

/**
 * Context for Alluxio audit logging.
 */
public interface AuditContext extends Closeable {

  /**
   * Set to true if the operation associated with this {@link AuditContext} is allowed, false
   * otherwise.
   *
   * @param allowed true if operation is allowed, false otherwise
   * @return {@link AuditContext} instance itself
   */
  AuditContext setAllowed(boolean allowed);

  /**
   * Set to true if the operration associated with this {@link AuditContext} is allowed and
   * succeeds.
   *
   * @param succeeded true if the operation has succeeded, false otherwise
   * @return {@link AuditContext} instance itself
   */
  AuditContext setSucceeded(boolean succeeded);

  @Override
  void close();
}
