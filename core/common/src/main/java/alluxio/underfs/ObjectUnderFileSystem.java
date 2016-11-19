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

import javax.annotation.concurrent.ThreadSafe;

/**
 * A object based abstract {@link UnderFileSystem}.
 */
@ThreadSafe
public abstract class ObjectUnderFileSystem extends BaseUnderFileSystem {
  /**
   * Constructs an {@link ObjectUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} used to create this ufs
   */
  protected ObjectUnderFileSystem(AlluxioURI uri) {
    super(uri);
  }

  @Override
  public boolean supportsFlush() {
    return false;
  }
}
