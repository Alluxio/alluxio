/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.ServerConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

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
   * @return a {@link FileSystem} client
   */
  public FileSystem getClient() throws IOException {
    final FileSystem fs = FileSystem.Factory.create(ServerConfiguration.global());
    mClients.add(fs);
    return fs;
  }

  /**
   * Returns a {@link FileSystem} client using a specific context. This client does not need to be
   * closed directly, but can be closed by calling {@link #close()} on this object.
   *
   * @param context the FileSystemContext to use
   * @return a {@link FileSystem} client
   */
  public FileSystem getClient(FileSystemContext context) throws IOException {
    final FileSystem fs = FileSystem.Factory.create(context);
    mClients.add(fs);
    return fs;
  }

  @Override
  public void close() throws IOException {
    synchronized (mClients) {
      mClients.forEach((client) -> {
        try {
          client.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      mClients.clear();
    }
  }
}
