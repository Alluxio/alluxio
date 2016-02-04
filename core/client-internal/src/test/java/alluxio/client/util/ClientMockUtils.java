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

package alluxio.client.util;

import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem;

/**
 * Utility methods for mocking the client.
 */
public final class ClientMockUtils {
  /**
   * Convenience method for mocking the {@link UnderFileSystem} for any ufsPath.
   *
   * @return the mocked {@link UnderFileSystem}
   */
  public static UnderFileSystem mockUnderFileSystem() {
    return mockUnderFileSystem(Mockito.anyString());
  }

  /**
   * When {@link UnderFileSystem#get(String, Configuration)} is called to get an
   * {@link UnderFileSystem}, it will instead return the mock returned by this method, as long as
   * `filename` matches `ufsPathMatcher`
   *
   * Because this method needs to mock a static method from {@link UnderFileSystem}, calling tests
   * should make sure to annotate their classes with `@PrepareForTesting(UnderFileSystem.class)`.
   *
   * @param ufsPathMatcher a {@link Mockito} String Matcher
   * @return the {@link UnderFileSystem} mock
   */
  public static UnderFileSystem mockUnderFileSystem(String ufsPathMatcher) {
    UnderFileSystem ufs = PowerMockito.mock(UnderFileSystem.class);
    PowerMockito.mockStatic(UnderFileSystem.class);
    PowerMockito.when(UnderFileSystem.get(ufsPathMatcher, Mockito.any(Configuration.class)))
        .thenReturn(ufs);
    return ufs;
  }

  private ClientMockUtils() {} // Utils class should not be instantiated
}
