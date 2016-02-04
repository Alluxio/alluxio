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

package alluxio.client.block;

import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import alluxio.client.BoundedStream;
import alluxio.client.Seekable;

/**
 * This class represents a stream to a block in the Tachyon system. The data source of the block
 * could be a local Tachyon worker, a remote Tachyon worker, or the under storage system. All
 * block streams provide data access to a sequential region of data of the block size of the block.
 */
@NotThreadSafe
// TODO(calvin): Resolve the confusion between Tachyon BufferedBlockInStream and BlockInStream.
public abstract class BlockInStream extends InputStream implements BoundedStream, Seekable {

}
