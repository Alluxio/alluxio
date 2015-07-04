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

package tachyon.worker.allocation;

/**
 * Define several AllocateStrategy, and get specific AllocateStrategy by AllocateStrategyType
 */
public class AllocateStrategies {

  /**
   * Get AllocateStrategy based on configuration
   *
   * @param strategyType configuration of AllocateStrategy
   * @return AllocationStrategy generated
   */
  public static AllocateStrategy getAllocateStrategy(AllocateStrategyType strategyType) {
    switch (strategyType) {
      case MAX_FREE:
        return new AllocateMaxFree();
      case RANDOM:
        return new AllocateRandom();
      case ROUND_ROBIN:
        return new AllocateRR();
      default:
        return new AllocateMaxFree();
    }
  }

  private AllocateStrategies() {}
}
