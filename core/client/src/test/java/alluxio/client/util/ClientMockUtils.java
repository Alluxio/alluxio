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

package alluxio.client.util;

import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem;

import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

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
