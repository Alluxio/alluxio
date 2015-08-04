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

package tachyon.worker.balancer;

/**
 * Factory of {@link Balancer} based on {@link BalancerType}
 */
public class BalancerFactory {
  /**
   * Create a new {@link Balancer}
   *
   * @param allocatorType AllocatorType which determines the class of allocator to create
   * @param view BlockMetadataManagerView to pass to Allocator
   * @return the generated Allocator
   */
  public static Balancer create(BalancerType getterType) {
    switch (getterType) {
      case DEFAULT:
        return new DefaultBalancer();
      case BALANCE:
        return new BalanceBalancer();
      default:
        return new DefaultBalancer();
    }
  }

  private BalancerFactory() {}
}

