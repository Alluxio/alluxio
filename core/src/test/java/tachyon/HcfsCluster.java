/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.IOException;

public class HcfsCluster extends UnderFileSystemCluster {
  private static final String HCFS_URI_KEY = "uri";
  private static String mUri = null;
  
  public HcfsCluster(String baseDir) {
    super(baseDir);
    String uri = System.getProperty(HCFS_URI_KEY);
    System.out.println("properties ::::::: " + uri);
    if (null != uri && !uri.equals("")) {
      mUri = uri;
    }
  }

  @Override
  public String getUnderFilesystemAddress() {
    return mUri + "tachyon_test";
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public void shutdown() throws IOException {
  }

  @Override
  public void start() throws IOException {
  }
}