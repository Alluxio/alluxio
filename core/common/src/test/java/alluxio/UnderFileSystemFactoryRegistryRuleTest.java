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

import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.junit.runners.model.Statement;

/**
 * Unit tests for {@link UnderFileSystemFactoryRegistryRule}.
 */
public class UnderFileSystemFactoryRegistryRuleTest {
  private UnderFileSystemFactory mUnderFileSystemFactory;

  private Statement mStatement = new Statement() {
    @Override
    public void evaluate() throws Throwable {
      Assert.assertEquals(mUnderFileSystemFactory, UnderFileSystemFactoryRegistry
          .find("mock://foo"));
    }
  };

  @Test
  public void testUnderFileSystemFactoryRegistryRule() throws Throwable {
    mUnderFileSystemFactory = Mockito.mock(UnderFileSystemFactory.class);
    Mockito.when(mUnderFileSystemFactory.supportsPath("mock://foo")).thenReturn(true);
    //check before
    Assert.assertEquals(null, UnderFileSystemFactoryRegistry.find("mock://foo"));
    new UnderFileSystemFactoryRegistryRule(mUnderFileSystemFactory)
        .apply(mStatement, null).evaluate();
    //check after
    Assert.assertEquals(null, UnderFileSystemFactoryRegistry.find("mock://foo"));
  }
}
