/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.client.file.cache.store;

import alluxio.exception.AlluxioException;

import java.nio.channels.WritableByteChannel;

/**
 * An exception that should be thrown when a page store implementation cannot find a page using
 * {@link alluxio.client.file.cache.PageStore#get(long, long, WritableByteChannel)} or
 * {@link alluxio.client.file.cache.PageStore#delete(long, long)}.
 */
public class PageNotFoundException extends AlluxioException {
  protected PageNotFoundException(Throwable cause) {
    super(cause);
  }

  protected PageNotFoundException(String cause) {
    super(cause);
  }
}
