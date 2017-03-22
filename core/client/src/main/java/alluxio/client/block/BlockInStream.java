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

package alluxio.client.block;

import alluxio.Seekable;
import alluxio.client.BoundedStream;

import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class represents a stream to a block in the Alluxio system. The data source of the block
 * could be a local Alluxio worker, a remote Alluxio worker, or the under storage system. All
 * block streams provide data access to a sequential region of data of the block size of the block.
 */
@NotThreadSafe
// TODO(calvin): Resolve the confusion between Alluxio BufferedBlockInStream and BlockInStream.
public abstract class BlockInStream extends InputStream implements BoundedStream, Seekable {

}
