/*
 * Licensed to IBM Ireland - Research and Development under one or more contributor license
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
package com.ibm.ie.tachyon.fuse;

import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;

import java.util.Optional;

/**
 *
 * @author Andrea Reale <realean2@ie.ibm.com>
 */
final class OpenFileEntry {
  // the tachyon file id
  public long tfid;
  public Optional<FileInStream> in;
  public Optional<FileOutStream> out;
}
