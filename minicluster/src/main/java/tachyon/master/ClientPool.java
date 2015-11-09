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

package tachyon.master;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Supplier;

import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory;
import tachyon.conf.TachyonConf;

/**
 * Keeps a collection of all clients ({@link tachyon.client.file.TachyonFileSystem}) returned. The
 * main reason for this is to build cleanup clients.
 */
public final class ClientPool implements Closeable {
  private final List<TachyonFileSystem> mClients =
      Collections.synchronizedList(new ArrayList<TachyonFileSystem>());

  ClientPool(Supplier<String> uriSupplier) {}

  /**
   * Returns a {@link tachyon.client.file.TachyonFileSystem} client. This client does not need to be
   * closed directly, but can be closed by calling {@link #close()} on this object.
   *
   * @param tachyonConf Tachyon configuration
   * @return a TachyonFS client
   * @throws IOException when the operation fails
   */
  public TachyonFileSystem getClient(TachyonConf tachyonConf) throws IOException {
    final TachyonFileSystem fs = TachyonFileSystemFactory.get();
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
