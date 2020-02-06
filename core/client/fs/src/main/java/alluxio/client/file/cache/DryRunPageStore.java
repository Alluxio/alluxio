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

package alluxio.client.file.cache;

import alluxio.exception.PageNotFoundException;

import org.apache.commons.io.input.NullInputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * A page store used in dry run mode to keep track of the blocks without writing any data.
 */
public class DryRunPageStore implements PageStore {
  private int mSize = 0;

  @Override
  public void put(PageId pageId, byte[] page) {
    // TODO(feng): update size accounting
    mSize++;
  }

  @Override
  public ReadableByteChannel get(PageId pageId, int pageOffset) {
    return Channels.newChannel(new NullInputStream(0));
  }

  @Override
  public void delete(PageId pageId) throws IOException, PageNotFoundException {
    mSize--;
  }

  @Override
  public int size() {
    return mSize;
  }

  @Override
  public void close() throws Exception {
    // no-op
  }
}
