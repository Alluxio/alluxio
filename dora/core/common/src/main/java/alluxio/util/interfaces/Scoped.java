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

package alluxio.util.interfaces;

import java.io.Closeable;

/**
 * Extension of Closeable which throws no checked exceptions. This is convenient for usage in
 * try-with-resources. We extend Closeable instead of AutoCloseable to enable use with Guava's
 * Closer.
 */
public interface Scoped extends Closeable {
  @Override
  void close();
}
