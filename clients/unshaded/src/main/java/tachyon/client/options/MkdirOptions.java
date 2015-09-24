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

package tachyon.client.options;

import tachyon.conf.TachyonConf;

public class MkdirOptions {
  public static class Builder {
    private boolean mRecursive;

    public Builder(TachyonConf conf) {
      mRecursive = false;
    }

    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
      return this;
    }

    public MkdirOptions build() {
      return new MkdirOptions(this);
    }
  }

  public static MkdirOptions defaults() {
    return new Builder(new TachyonConf()).build();
  }

  private final boolean mRecursive;

  private MkdirOptions(MkdirOptions.Builder builder) {
    mRecursive = builder.mRecursive;
  }

  public boolean isRecursive() {
    return mRecursive;
  }
}
