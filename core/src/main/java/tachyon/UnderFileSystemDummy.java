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

package tachyon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import tachyon.conf.TachyonConf;

/**
 * dummy UnderFilesystem implementation.
 * 
 * This is used when we use Tachyon as pure cache without any backing store
 */
public class UnderFileSystemDummy extends UnderFileSystemSingleLocal {

  protected UnderFileSystemDummy(TachyonConf tachyonConf) {
    super(tachyonConf);
  }

  public static UnderFileSystem getClient(TachyonConf tachyonConf) {
    return new UnderFileSystemDummy(tachyonConf);
  }

  @Override
  public OutputStream create(String path) throws IOException {
    return new DummyFileOutputStream();
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return new DummyFileOutputStream();
  }

  @Override
  public OutputStream create(String path, short replication, int blockSizeByte) throws IOException {
    return new DummyFileOutputStream();
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    return true;
  }

/*
  @Override
  public InputStream open(String path) throws IOException {
    if (exists(path)) {
      super.open(path);
    }
    return new DummyFileInputStream();
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    return true;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    return true;
  }


  @Override
  public boolean exists(String path) throws IOException {
    return false;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    if (super.exists(path)) {
      return super.getBlockSizeByte(path);
    }
    return 0;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    if (super.exists(path)) {
      return super.getFileSize(path);
    }
    return 0;
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    if (super.exists(path)) {
      return super.getModificationTimeMs(path);
    }
    return 0;
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    if (super.exists(path)) {
      return super.getSpace(path, type);
    }
    return 0;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    if (super.exists(path)) {
      return super.isFile(path);
    }
    return true;
  }

  @Override
  public String[] list(String path) throws IOException {
    if (super.exists(path)) {
      super.list(path);
    }
    return new String[0];
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return true;
  }

  @Override
  public void setPermission(String path, String posixPerm) throws IOException {}
*/
}
