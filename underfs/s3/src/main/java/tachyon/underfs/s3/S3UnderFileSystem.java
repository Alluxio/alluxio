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

package tachyon.underfs.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;

public class S3UnderFileSystem extends UnderFileSystem {

  private final AmazonS3 mS3Client;

  public S3UnderFileSystem(TachyonConf tachyonConf) {
    super(tachyonConf);
    BasicAWSCredentials credentials =
        new BasicAWSCredentials(tachyonConf.get("fs.s3n.awsAccessKeyId", null),
        tachyonConf.get("fs.s3n.awsSecretAccessKey", null));
    mS3Client = new AmazonS3Client(credentials);
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void connectFromMaster(TachyonConf conf, String hostname) {

  }

  @Override
  public void connectFromWorker(TachyonConf conf, String hostname) {

  }

  @Override
  public OutputStream create(String path) throws IOException {
    return null;
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return null;
  }

  @Override
  public OutputStream create(String path, short replication, int blockSizeByte)
      throws IOException {
    return null;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    return false;
  }

  @Override
  public boolean exists(String path) throws IOException {
    return false;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return -1L;
  }

  @Override
  public Object getConf() {
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return null;
  }

  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    return -1L;
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    return -1L;
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1L;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return false;
  }

  @Override
  public String[] list(String path) throws IOException {
    return null;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return false;
  }

  @Override
  public InputStream open(String path) throws IOException {
    return null;
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    return false;
  }

  @Override
  public void setConf(Object conf) {

  }

  public void setPermission(String path, String posixPerm) throws IOException {

  }

}
