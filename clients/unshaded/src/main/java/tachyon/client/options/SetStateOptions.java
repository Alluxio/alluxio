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
import java.util.Optional;

public class SetStateOptions {
  public static class Builder {
    private Optional<Boolean> mPinned;

    public Builder(TachyonConf conf) {
      mPinned = Optional.of(false);
    }

    public Builder setRecursive(boolean recursive) {
      mPinned = Optional.of(recursive);
      return this;
    }

    public SetStateOptions build() {
      return new SetStateOptions(this);
    }
  }

  public static SetStateOptions defaults() {
    return new Builder(new TachyonConf()).build();
  }

  private final Optional<Boolean> mPinned;

  private SetStateOptions(SetStateOptions.Builder builder) {
    mPinned = Optional.of(builder.mPinned.get());
  }

  public Optional<Boolean> getPinned() { return mPinned; }
}
