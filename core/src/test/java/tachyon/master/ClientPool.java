/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Supplier;

import tachyon.client.TachyonFS;

/**
 * Keeps a collection of all clients ({@link tachyon.client.TachyonFS}) returned. The main reason
 * for this is to build cleanup clients.
 */
public final class ClientPool implements Closeable {
  private final Supplier<String> URI_SUPPLIER;

  private final List<TachyonFS> CLIENTS = Collections.synchronizedList(new ArrayList<TachyonFS>());

  ClientPool(Supplier<String> uriSupplier) {
    URI_SUPPLIER = uriSupplier;
  }

  /**
   * Returns a {@link tachyon.client.TachyonFS} client. This client does not need to be closed
   * directly, but can be closed by calling {@link #close()} on this object.
   */
  public TachyonFS getClient() throws IOException {
    final TachyonFS fs = TachyonFS.get(URI_SUPPLIER.get());
    CLIENTS.add(fs);
    return fs;
  }

  @Override
  public void close() throws IOException {
    synchronized (CLIENTS) {
      for (TachyonFS fs : CLIENTS) {
        fs.close();
      }

      CLIENTS.clear();
    }
  }
}
