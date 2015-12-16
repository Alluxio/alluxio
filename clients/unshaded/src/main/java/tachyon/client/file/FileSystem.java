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

import tachyon.Path;
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
  FileOutStream create(Path path);
  FileOutStream create(Path path, CreateOptions options);
  Path createDirectory(Path path);
  Path createDirectory(Path path, MkdirOptions options);
  void delete(Path path);
  void delete(Path path, DeleteOptions options);
  boolean exists(Path path);
  void free(Path path);
  void free(Path path, FreeOptions options);
  FileInfo getStatus(Path path);
  FileInfo getStatus(Path path, GetInfoOptions options);
  List<FileInfo> listStatus(Path path);
  List<FileInfo> listStatus(Path path, ListStatusOptions options);
  Path loadMetadata(Path path);
  Path loadMetadata(Path path, LoadMetadataOptions options);
  void mount(Path src, Path dst);
  void mount(Path src, Path dst, MountOptions options);
  FileInStream open(Path path);
  FileInStream open(Path path, OpenOptions options);
  void rename(Path src, Path dst);
  void rename(Path src, Path dst, RenameOptions options);
  void setAttr(Path path);
  void setAttr(Path path, SetStateOptions options);
  void unmount(Path path);
  void unmount(Path path, UnmountOptions options);
}
