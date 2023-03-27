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

import alluxio.file.options.DescendantType;

import java.util.function.Consumer;
import javax.annotation.Nullable;

interface UfsClient {

  void performQueryAsync(
      String path, @Nullable String continuationToken, DescendantType descendantType,
      Consumer<UfsLoadResult> onComplete, Consumer<Throwable> onError);
}
