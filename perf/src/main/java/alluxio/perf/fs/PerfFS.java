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

package alluxio.perf.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface PerfFS {

  public abstract void close() throws IOException;

  public abstract OutputStream create(String path) throws IOException;

  public abstract OutputStream create(String path, int blockSize) throws IOException;

  public abstract OutputStream create(String path, int blockSize, String writeType)
      throws IOException;

  public abstract boolean createEmptyFile(String path) throws IOException;

  public abstract boolean delete(String path, boolean recursive) throws IOException;

  public abstract boolean exists(String path) throws IOException;

  public abstract long getLength(String path) throws IOException;

  public abstract boolean isDirectory(String path) throws IOException;

  public abstract boolean isFile(String path) throws IOException;

  public abstract List<String> listFullPath(String path) throws IOException;

  public abstract boolean mkdirs(String path, boolean createParent) throws IOException;

  public abstract InputStream open(String path) throws IOException;

  public abstract InputStream open(String path, String readType) throws IOException;

  public abstract boolean rename(String src, String dst) throws IOException;
}
