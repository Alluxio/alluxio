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
package tachyon.master;

import java.io.IOException;

import tachyon.Constants;
import tachyon.UnderFileSystem;

/**
 * The Journal manages Tachyon image and journal log.
 */
public class Journal {
  private EditLog mEditLog = new EditLog(null, true, 0);

  private int mCurrentLogFileNum = 0;
  private String mImagePath = null;
  private String mStandbyImagePath = null;
  private String mEditLogPath = null;

  /**
   * Create a Journal manager.
   * 
   * @param folder
   *          the folder contains image file and edit log files.
   * @param imageFileName
   *          image file name
   * @param editLogFileName
   *          edit file name
   * @throws IOException
   */
  public Journal(String folder, String imageFileName, String editLogFileName) throws IOException {
    if (!folder.endsWith(Constants.PATH_SEPARATOR)) {
      folder += Constants.PATH_SEPARATOR;
    }
    mImagePath = folder + imageFileName;
    mEditLogPath = folder + editLogFileName;
  }

  /**
   * Close down the edit log
   */
  public void close() {
    if (mEditLog != null) {
      mEditLog.close();
    }
  }

  /**
   * Create an edit log.
   * 
   * @param startingTransactionId
   *          the starting transaction id of the edit log.
   * @throws IOException
   */
  public void createEditLog(long startingTransactionId) throws IOException {
    mEditLog = new EditLog(mEditLogPath, false, startingTransactionId);
  }

  public void createImage(MasterInfo info) throws IOException {
    if (mStandbyImagePath == null) {
      Image.create(info, mImagePath);
      EditLog.markUpToDate(mEditLogPath);
    } else {
      Image.rename(mStandbyImagePath, mImagePath);
    }
  }

  public void createImage(MasterInfo info, String imagePath) throws IOException {
    Image.create(info, imagePath);
    mStandbyImagePath = imagePath;
  }

  public EditLog getEditLog() {
    return mEditLog;
  }

  /**
   * Get image file's last modification time.
   * 
   * @return the last modification time in millisecond.
   * @throws IOException
   */
  public long getImageModTimeMs() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mImagePath);
    if (!ufs.exists(mImagePath)) {
      return -1;
    }
    return ufs.getModificationTimeMs(mImagePath);
  }

  /**
   * Load edit log.
   * 
   * @param info
   *          The Master Info.
   * @return The last transaction id.
   * @throws IOException
   */
  public long loadEditLog(MasterInfo info) throws IOException {
    return EditLog.load(info, mEditLogPath, mCurrentLogFileNum);
  }

  /**
   * Load image file.
   * 
   * @param info
   *          The Master Info.
   * @throws IOException
   */
  public void loadImage(MasterInfo info) throws IOException {
    Image.load(info, mImagePath);
  }

  public void loadSingleLogFile(MasterInfo info, String path) throws IOException {
    EditLog.loadSingleLog(info, path);
    mCurrentLogFileNum ++;
  }

  /**
   * Changes the max edit log size for testing purposes
   * 
   * @param size
   */
  public void setMaxLogSize(int size) {
    mEditLog.setMaxLogSize(size);
  }
}
