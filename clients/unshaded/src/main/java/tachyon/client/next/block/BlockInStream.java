/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.next.block;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provides a stream API to read a block from Tachyon. An instance of this extending class can be
 * obtained by calling {@link TachyonBS#getInStream}. Multiple BlockInStreams can be opened for a
 * block. This class is thread safe.
 *
 * The type of BlockInStream returned will depend on the data location and user configuration. A
 * {@link LocalBlockInStream} is returned if the data can be directly read from the local storage
 * of the machine the client is on and the user has enabled this optimization. Otherwise, a
 * {@link RemoteBlockInStream} will be returned which will read the data through a Tachyon worker.
 */
public abstract class BlockInStream extends InputStream {
  // TODO: Implement me
  public abstract void seek(long pos) throws IOException;
}
