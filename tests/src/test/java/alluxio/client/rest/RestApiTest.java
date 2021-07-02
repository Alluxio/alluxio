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

package alluxio.client.rest;

import alluxio.testutils.BaseIntegrationTest;

import java.util.HashMap;
import java.util.Map;

public abstract class RestApiTest extends BaseIntegrationTest {
  protected static final Map<String, String> NO_PARAMS = new HashMap<>();

  protected String mHostname;
  protected int mPort;
  protected String mServicePrefix;

  protected String getEndpoint(String suffix) {
    return mServicePrefix + "/" + suffix;
  }
}
