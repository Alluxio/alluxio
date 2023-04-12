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

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.file.options.DescendantType;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsLoadResult;
import alluxio.underfs.UfsStatus;
import alluxio.util.RateLimiter;

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
  UfsStatus mUfsStatus = null;
  RateLimiter mRateLimiter = null;

  void setError(@Nullable Throwable t) {
    mError = t;
  }

  void setRateLimiter(RateLimiter rateLimiter) {
    mRateLimiter = rateLimiter;
  }

  void setResult(Iterator<Stream<UfsStatus>> items) {
    mItems = items;
  }

  void setGetStatusItem(UfsStatus item) {
    mUfsStatus = item;
  }

  void setResultFunc(Function<String, Pair<Stream<UfsStatus>, Boolean>> resultFunc) {
    mResultFunc = resultFunc;
  }

  @Override
  public void performGetStatusAsync(
      String path, Consumer<UfsLoadResult> onComplete, Consumer<Throwable> onError) {
    onComplete.accept(new UfsLoadResult(
        mUfsStatus == null ? Stream.empty() : Stream.of(mUfsStatus),
        mUfsStatus == null ? 0 : 1,
        null, null, false,
        mUfsStatus != null && mUfsStatus.isFile(), true));
  }

  @Override
  public void performListingAsync(
      String path, @Nullable String continuationToken, @Nullable String startAfter,
      DescendantType descendantType,
      Consumer<UfsLoadResult> onComplete, Consumer<Throwable> onError) {
    if (mError != null) {
      onError.accept(mError);
    } else if (mResultFunc != null) {
      try {
        Pair<Stream<UfsStatus>, Boolean> result = mResultFunc.apply(path);
        List<UfsStatus> items = result.getFirst().collect(Collectors.toList());
        AlluxioURI lastItem = new AlluxioURI(items.get(items.size() - 1).getName());
        onComplete.accept(new UfsLoadResult(items.stream(), items.size(),
            continuationToken, lastItem, result.getSecond(),
            items.size() > 0 && items.get(0).isFile(), true));
      } catch (Throwable t) {
        onError.accept(t);
      }
    } else {
      if (mItems.hasNext()) {
        List<UfsStatus> items = mItems.next().collect(Collectors.toList());
        AlluxioURI lastItem = new AlluxioURI(items.get(items.size() - 1).getName());
        onComplete.accept(new UfsLoadResult(items.stream(), items.size(),
            continuationToken, lastItem, mItems.hasNext(),
            items.size() > 0 && items.get(0).isFile(), true));
      }
    }
  }

  @Override
  public RateLimiter getRateLimiter() {
    if (mRateLimiter == null) {
      return RateLimiter.createRateLimiter(0);
    }
    return mRateLimiter;
  }
}
