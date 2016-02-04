/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client.lineage.options;

import javax.annotation.concurrent.NotThreadSafe;

import alluxio.annotation.PublicApi;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * The method option for retrieving a list of lineage information.
 */
@PublicApi
@NotThreadSafe
public final class GetLineageInfoListOptions {
  /**
   * @return the default options
   */
  @SuppressFBWarnings("ISC_INSTANTIATE_STATIC_CLASS")
  public static GetLineageInfoListOptions defaults() {
    return new GetLineageInfoListOptions();
  }

  private GetLineageInfoListOptions() {
    // No options currently
  }
}
