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

package alluxio.refresh;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Policy for determining whether a refresh should be performed. Calls to attempt return
 * immediately and do not sleep until refresh.
 */
@NotThreadSafe
public interface RefreshPolicy {

  /**
   * Checks if it is time to perform the next refresh and returns immediately.
   *
   * @return whether a refresh should be performed
   */
  boolean attempt();
}
