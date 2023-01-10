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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Test;
import org.junit.runners.model.Statement;

/**
 * Unit tests for {@link UnderFileSystemFactoryRegistryRule}.
 */
public class UnderFileSystemFactoryRegistryRuleTest {
  private static final String UFS_PATH = "test://foo";

  private UnderFileSystemFactory mUnderFileSystemFactory;
  private final AlluxioConfiguration mConfiguration = Configuration.global();

  private final Statement mStatement = new Statement() {
    @Override
    public void evaluate() throws Throwable {
      assertEquals(mUnderFileSystemFactory, UnderFileSystemFactoryRegistry
          .find(UFS_PATH, mConfiguration));
    }
  };

  @Test
  public void testUnderFileSystemFactoryRegistryRule() throws Throwable {
    mUnderFileSystemFactory = mock(UnderFileSystemFactory.class);
    when(
        mUnderFileSystemFactory.supportsPath(eq(UFS_PATH), any(UnderFileSystemConfiguration.class)))
            .thenReturn(true);
    // check before
    assertNull(UnderFileSystemFactoryRegistry.find(UFS_PATH, mConfiguration));
    new UnderFileSystemFactoryRegistryRule(mUnderFileSystemFactory)
        .apply(mStatement, null).evaluate();
    // check after
    assertNull(UnderFileSystemFactoryRegistry.find(UFS_PATH, mConfiguration));
  }
}
