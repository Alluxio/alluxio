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

package alluxio;

import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * Base class used for specifying the maximum time a test should run for.
 */
public abstract class BaseIntegrationTest {
  @Rule
  public Timeout mGlobalTimeout = Timeout.seconds(600);
}
