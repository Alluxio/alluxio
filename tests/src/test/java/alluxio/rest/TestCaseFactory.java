/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.rest;

import alluxio.LocalAlluxioClusterResource;

import java.util.Map;

/**
 * Factory for creating instances of {@link TestCase}.
 */
public class TestCaseFactory {
  public static final String MASTER_SERVICE = "master";
  public static final String WORKER_SERVICE = "worker";

  /**
   * Creates a new instance of {@link TestCase} for the master service.
   *
   * @param suffix the suffix to use
   * @param parameters the parameters to use
   * @param method the method to use
   * @param expectedResult the expected result to use; null implies empty response
   * @param resource the local Alluxio cluster resource
   * @return a REST API test case
   */
  public static TestCase newMasterTestCase(String suffix, Map<String, String> parameters,
      String method, Object expectedResult, LocalAlluxioClusterResource resource) {
    return new TestCase(suffix, parameters, method, expectedResult, MASTER_SERVICE,
        resource);
  }

  /**
   * Creates a new instance of {@link TestCase} for the worker service.
   *
   * @param suffix the suffix to use
   * @param parameters the parameters to use
   * @param method the method to use
   * @param expectedResult the expected result to use; null implies empty response
   * @param resource the local Alluxio cluster resource
   * @return a REST API test case
   */
  public static TestCase newWorkerTestCase(String suffix, Map<String, String> parameters,
      String method, Object expectedResult, LocalAlluxioClusterResource resource) {
    return new TestCase(suffix, parameters, method, expectedResult, WORKER_SERVICE,
        resource);
  }
}
