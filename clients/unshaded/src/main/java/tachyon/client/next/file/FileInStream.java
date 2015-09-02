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

package tachyon.client.next.file;

import tachyon.client.next.InStream;

/**
 * Provides a stream API to read a file from Tachyon. An instance of this class can be obtained by
 * calling {@link TachyonFileSystem#getInStream}. Multiple FileInStreams can be opened for a
 * {@link TachyonFile}. This class is not thread safe and should only be used by one thread.
 */
public abstract class FileInStream extends InStream {
  // TODO: Add FileInStream specific logic here
}
