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

package tachyon.client.file.policy;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import tachyon.client.block.BlockWorkerInfo;

/**
 * Tests {@link LocationPolicyRegistry}.
 */
public final class LocationPolicyRegistryTest {
  private Set<Class<? extends FileWriteLocationPolicy>> mExpectedPolicyClasses;

  @Before
  public void before() {
    mExpectedPolicyClasses = Sets.newHashSet();
    mExpectedPolicyClasses.add(LocalFirstPolicy.class);
    mExpectedPolicyClasses.add(MostAvailableFirstPolicy.class);
    mExpectedPolicyClasses.add(RoundRobinPolicy.class);
  }

  @Test
  public void autoDiscoveryTest() {
    Assert.assertEquals(mExpectedPolicyClasses, LocationPolicyRegistry.availablePolices());
  }

  static class DummyPolicy implements FileWriteLocationPolicy {

    @Override
    public String getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList, long blockSizeBytes) {
      throw new UnsupportedOperationException();
    }
  }

  static class DummyPolicyFactory implements FileWriteLocationPolicyFactory<DummyPolicy> {

    @Override
    public DummyPolicy create(List<BlockWorkerInfo> workerInfoList,
        FileWriteLocationPolicyOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<DummyPolicy> getPolicyClass() {
      return DummyPolicy.class;
    }
  }

  @Test
  public void registerNewFactoryTest() {
    mExpectedPolicyClasses.add(DummyPolicy.class);
    LocationPolicyRegistry.register(new DummyPolicyFactory());
    Assert.assertEquals(mExpectedPolicyClasses, LocationPolicyRegistry.availablePolices());

    // unregister the policy
    LocationPolicyRegistry.unregister(DummyPolicy.class);
    mExpectedPolicyClasses.remove(DummyPolicy.class);
    Assert.assertEquals(mExpectedPolicyClasses, LocationPolicyRegistry.availablePolices());
  }
}
