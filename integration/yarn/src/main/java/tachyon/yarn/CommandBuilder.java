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

package tachyon.yarn;

import java.util.Vector;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

/**
 * This class is used to build a string representing a shell command by adding arguments.
 *
 * TODO(binfan): to share this class into util.
 */
@NotThreadSafe
public class CommandBuilder {
  private String mBase;
  private Vector<String> mArgs = new Vector<String>(10);

  public CommandBuilder(String base) {
    mBase = Preconditions.checkNotNull(base);
  }

  public CommandBuilder addArg(Object arg) {
    mArgs.add(String.valueOf(arg));
    return this;
  }

  public CommandBuilder addArg(String opt, Object arg) {
    mArgs.add(opt + " " + String.valueOf(arg));
    return this;
  }

  // Get final command
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(mBase + " ");
    for (String str : mArgs) {
      builder.append(str).append(" ");
    }
    return builder.toString();
  }
}
