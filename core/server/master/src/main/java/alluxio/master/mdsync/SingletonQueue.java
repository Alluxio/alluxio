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

package alluxio.master.mdsync;

import alluxio.exception.runtime.UnimplementedRuntimeException;

import java.util.AbstractQueue;
import java.util.Collections;
import java.util.Iterator;

class SingletonQueue<T> extends AbstractQueue<T> {

  T mItem;

  @Override
  public Iterator<T> iterator() {
    if (mItem == null) {
      return Collections.emptyIterator();
    }
    return Collections.singletonList(mItem).iterator();
  }

  @Override
  public int size() {
    return mItem == null ? 0 : 1;
  }

  @Override
  public boolean offer(T t) {
    throw new UnimplementedRuntimeException("unsupported");
  }

  @Override
  public T poll() {
    T result = mItem;
    mItem = null;
    return result;
  }

  @Override
  public T peek() {
    return mItem;
  }
}
