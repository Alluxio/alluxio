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

package alluxio.security;

import alluxio.security.group.CachedGroupMapping;
import alluxio.security.group.GroupMappingService;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

/**
 * Test utils to reset cache in GroupMappingService.
 */
@PrepareForTest(GroupMappingService.Factory.class)
public final class GroupMappingServiceTestUtils {
  /**
   * Resets the cache for GroupMappingService.
   */
  public static void resetCache() {
    Whitebox.setInternalState(GroupMappingService.Factory.class, "sCachedGroupMapping",
        (CachedGroupMapping) null);
  }
}
