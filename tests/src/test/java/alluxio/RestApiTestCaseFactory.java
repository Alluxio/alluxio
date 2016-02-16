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

package alluxio;

import java.util.Map;

/**
 * Factory for creating instances of {@link RestApiTestCase}.
 */
public class RestApiTestCaseFactory {
  public static final String MASTER_SERVICE = "master";
  public static final String WORKER_SERVICE = "worker";

  /**
   * Creates a new instance of {@link RestApiTestCaseFactory} for the master service.
   *
   * @param suffix the suffix to use
   * @param parameters the parameters to use
   * @param method the method to use
   * @param expectedResult the expected result to use
   * @param resource the local Alluxio cluster resource
   * @return a REST API test case
   */
  public static RestApiTestCase newMasterTestCase(String suffix, Map<String, String> parameters,
      String method, Object expectedResult, LocalAlluxioClusterResource resource) {
    return new RestApiTestCase(suffix, parameters, method, expectedResult, MASTER_SERVICE,
        resource);
  }

  /**
   * Creates a new instance of {@link RestApiTestCaseFactory} for the worker service.
   *
   * @param suffix the suffix to use
   * @param parameters the parameters to use
   * @param method the method to use
   * @param expectedResult the expected result to use
   * @param resource the local Alluxio cluster resource
   * @return a REST API test case
   */
  public static RestApiTestCase newWorkerTestCase(String suffix, Map<String, String> parameters,
      String method, Object expectedResult, LocalAlluxioClusterResource resource) {
    return new RestApiTestCase(suffix, parameters, method, expectedResult, WORKER_SERVICE,
        resource);
  }
}
