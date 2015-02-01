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

import java.util.Random;

import tachyon.worker.hierarchy.StorageDir;

/**
 * Allocate space on StorageDirs randomly
 */
public class AllocateRandom extends AllocateStrategyBase {
  private Random mRandm = new Random(System.currentTimeMillis());

  @Override
  public StorageDir getStorageDir(StorageDir[] storageDirs, long userId, long requestBytes) {
    int i = mRandm.nextInt(storageDirs.length);
    for (int j = 0; j < storageDirs.length; j ++, i ++) {
      i = i % storageDirs.length;
      StorageDir dir = storageDirs[i];
      if (dir.getAvailableBytes() >= requestBytes) {
        if (dir.requestSpace(userId, requestBytes)) {
          return dir;
        }
      }
    }
    return null;
  }
}
