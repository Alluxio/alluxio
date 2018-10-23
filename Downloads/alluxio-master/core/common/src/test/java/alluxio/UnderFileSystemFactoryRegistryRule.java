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

public final class UnderFileSystemFactoryRegistryRule extends AbstractResourceRule {
  private UnderFileSystemFactory mFactory;

  /**
   * @param factory the UnderFileSystemFactory type variable for register and unregister
   */
  public UnderFileSystemFactoryRegistryRule(UnderFileSystemFactory factory) {
    mFactory = factory;
  }

  @Override
  public void before() throws Exception {
    UnderFileSystemFactoryRegistry.register(mFactory);
  }

  @Override
  public void after() {
    UnderFileSystemFactoryRegistry.unregister(mFactory);
  }
}
