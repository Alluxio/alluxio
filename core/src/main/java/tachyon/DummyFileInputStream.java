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

package tachyon;

import java.io.InputStream;

public class DummyFileInputStream extends InputStream {

  public DummyFileInputStream() {
  }

  public int read() {
    return 1;
  }

  public int read(byte[] b) {
    return b.length;
  }

  public int read(byte[] b, int off, int len) {
    return len;
  }

  public void close() {
  }

  public long skip(long offset) {
    return offset;
  }
}

