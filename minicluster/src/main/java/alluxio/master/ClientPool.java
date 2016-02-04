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

package alluxio.master;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Supplier;

import alluxio.client.file.FileSystem;
import alluxio.Configuration;

/**
 * Keeps a collection of all clients ({@link FileSystem}) returned. The main reason for this is
 * to build cleanup clients.
 */
@ThreadSafe
public final class ClientPool implements Closeable {
  private final List<FileSystem> mClients =
      Collections.synchronizedList(new ArrayList<FileSystem>());

  ClientPool(Supplier<String> uriSupplier) {}

  /**
   * Returns a {@link FileSystem} client. This client does not need to be
   * closed directly, but can be closed by calling {@link #close()} on this object.
   *
   * @param configuration Tachyon configuration
   * @return a TachyonFS client
   * @throws IOException when the operation fails
   */
  public FileSystem getClient(Configuration configuration) throws IOException {
    final FileSystem fs = FileSystem.Factory.get();
    mClients.add(fs);
    return fs;
  }

  @Override
  public void close() throws IOException {
    synchronized (mClients) {
      mClients.clear();
    }
  }
}
