/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.IOException;

/**
 * The Journal manages Tachyon image and journal log.
 */
public class Journal {
  private EditLog mEditLog = new EditLog(null, true, 0);

  private String mImagePath;
  private String mEditLogPath;

  public Journal(String folder, String imageFileName, String editLogFileName) throws IOException {
    if (!folder.endsWith("/")) {
      folder += "/";
    }
    mImagePath = folder + imageFileName;
    mEditLogPath = folder + editLogFileName;
  }

  public void loadImage(MasterInfo info) throws IOException {
    Image.load(info, mImagePath);
  }

  /**
   * Load edit log.
   * @param info The Master Info.
   * @return The last transaction id.
   * @throws IOException
   */
  public long loadEditLog(MasterInfo info) throws IOException {
    return EditLog.load(info, mEditLogPath);
  }

  public void createImage(MasterInfo info) throws IOException {
    Image.create(info, mImagePath);
  }

  public void createEditLog(long transactionId) throws IOException {
    mEditLog = new EditLog(mEditLogPath, false, transactionId);
  }

  public EditLog getEditLog() {
    return mEditLog;
  }

  /* Close down the edit log */
  public void close() {
    if (mEditLog != null) {
      mEditLog.close();
    }
  }
}
