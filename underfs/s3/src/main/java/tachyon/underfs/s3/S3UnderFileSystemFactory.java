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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemFactory;

public class S3UnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public UnderFileSystem create(String path, TachyonConf tachyonConf, Object conf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    if (addAndCheckAWSCredentials(tachyonConf)) {
      TachyonURI uri = new TachyonURI(path);
      return new S3UnderFileSystem(uri.getHost(), tachyonConf);
    } else {
      String err = "AWS Credentials not available, cannot create S3 Under File System.";
      LOG.error(err);
      throw Throwables.propagate(new IOException(err));
    }
  }

  @Override
  public boolean supportsPath(String path, TachyonConf tachyonConf) {
    return path != null && (path.startsWith("s3://") || path.startsWith("s3n://"));
  }

  private boolean addAndCheckAWSCredentials(TachyonConf tachyonConf) {
    String accessKeyConf = "fs.s3n.awsAccessKeyId";
    if (System.getProperty(accessKeyConf) != null && tachyonConf.get(accessKeyConf, null) == null) {
      tachyonConf.set(accessKeyConf, System.getProperty(accessKeyConf));
    }
    String secretKeyConf = "fs.s3n.awsSecretAccessKey";
    if (System.getProperty(secretKeyConf) != null && tachyonConf.get(secretKeyConf, null) == null) {
      tachyonConf.set(secretKeyConf, System.getProperty(secretKeyConf));
    }
    return tachyonConf.get(accessKeyConf, null) != null
        && tachyonConf.get(secretKeyConf, null) != null;
  }
}
