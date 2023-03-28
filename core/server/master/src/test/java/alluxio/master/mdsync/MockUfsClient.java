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

import alluxio.collections.Pair;
import alluxio.file.options.DescendantType;
import alluxio.underfs.UfsStatus;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class MockUfsClient implements UfsClient {

  Throwable mError = null;
  Iterator<Stream<UfsStatus>> mItems = null;
  Function<String, Pair<Stream<UfsStatus>, Boolean>> mResultFunc = null;

  void setError(@Nullable Throwable t) {
    mError = t;
  }

  void setResult(Iterator<Stream<UfsStatus>> items) {
    mItems = items;
  }

  void setResultFunc(Function<String, Pair<Stream<UfsStatus>, Boolean>> resultFunc) {
    mResultFunc = resultFunc;
  }

  @Override
  public void performQueryAsync(
      String path, @Nullable String continuationToken, DescendantType descendantType,
      Consumer<UfsLoadResult> onComplete, Consumer<Throwable> onError) {
    if (mError != null) {
      onError.accept(mError);
    } else if (mResultFunc != null) {
      try {
        Pair<Stream<UfsStatus>, Boolean> result = mResultFunc.apply(path);
        List<UfsStatus> items = result.getFirst().collect(Collectors.toList());
        onComplete.accept(new UfsLoadResult(items.stream(), items.size(),
            continuationToken, result.getSecond()));
      } catch (Throwable t) {
        onError.accept(t);
      }
    } else {
      if (mItems.hasNext()) {
        List<UfsStatus> items = mItems.next().collect(Collectors.toList());
        onComplete.accept(new UfsLoadResult(items.stream(), items.size(),
            continuationToken, mItems.hasNext()));
      }
    }
  }
}
