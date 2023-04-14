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

package alluxio;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Closeable supplier which supports lazy initialization and cleanup if initialized.
 *
 * @param <T> the lazy initializing object
 */
public class CloseableSupplier<T extends Closeable> implements Supplier<T>, Closeable {
  private final Supplier<T> mDelegate;
  private T mInstance;

  /**
   * @param delegate the underlying supplier
   */
  public CloseableSupplier(Supplier<T> delegate) {
    mDelegate = delegate;
  }

  @Override
  public synchronized T get() {
    if (mInstance == null) {
      mInstance = mDelegate.get();
    }
    return mInstance;
  }

  @Override
  public synchronized void close() throws IOException {
    if (mInstance != null) {
      mInstance.close();
      mInstance = null;
    }
  }
}
