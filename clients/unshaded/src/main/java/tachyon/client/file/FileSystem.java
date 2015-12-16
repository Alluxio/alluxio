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

package tachyon.client.file;

import tachyon.TachyonURI;
import tachyon.client.file.options.CreateOptions;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.FreeOptions;
import tachyon.client.file.options.GetInfoOptions;
import tachyon.client.file.options.ListStatusOptions;
import tachyon.client.file.options.LoadMetadataOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.MountOptions;
import tachyon.client.file.options.OpenOptions;
import tachyon.client.file.options.RenameOptions;
import tachyon.client.file.options.SetStateOptions;
import tachyon.client.file.options.UnmountOptions;
import tachyon.thrift.FileInfo;

import java.util.List;

interface FileSystem {
  TachyonURI createDirectory(TachyonURI path);

  TachyonURI createDirectory(TachyonURI path, MkdirOptions options);

  FileOutStream createFile(TachyonURI path);

  FileOutStream createFile(TachyonURI path, CreateOptions options);

  void delete(TachyonURI path);

  void delete(TachyonURI path, DeleteOptions options);

  boolean exists(TachyonURI path);

  void free(TachyonURI path);

  void free(TachyonURI path, FreeOptions options);

  PathStatus getStatus(TachyonURI path);

  PathStatus getStatus(TachyonURI path, GetInfoOptions options);

  List<FileInfo> listStatus(TachyonURI path);

  List<FileInfo> listStatus(TachyonURI path, ListStatusOptions options);

  TachyonURI loadMetadata(TachyonURI path);

  TachyonURI loadMetadata(TachyonURI path, LoadMetadataOptions options);

  void mount(TachyonURI src, TachyonURI dst);

  void mount(TachyonURI src, TachyonURI dst, MountOptions options);

  FileInStream openFile(TachyonURI path);

  FileInStream openFile(TachyonURI path, OpenOptions options);

  void rename(TachyonURI src, TachyonURI dst);

  void rename(TachyonURI src, TachyonURI dst, RenameOptions options);

  void setAttribute(TachyonURI path);

  void setAttribute(TachyonURI path, SetStateOptions options);

  void unmount(TachyonURI path);

  void unmount(TachyonURI path, UnmountOptions options);
}
