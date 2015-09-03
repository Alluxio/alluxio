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

package tachyon.master;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;

/**
 * The Journal manages Tachyon image and journal log.
 */
public class Journal {
  private EditLog mEditLog;

  private int mCurrentLogFileNum = 0;
  private String mImagePath = null;
  private String mStandbyImagePath = null;
  private String mEditLogPath = null;
  private final TachyonConf mTachyonConf;

  /**
   * Create a Journal manager.
   *
   * @param folder the folder contains image file and edit log files.
   * @param imageFileName image file name
   * @param editLogFileName edit file name
   * @param conf Tachyon configuration
   * @throws IOException when the operation fails
   */
  public Journal(String folder, String imageFileName, String editLogFileName, TachyonConf conf)
      throws IOException {
    mTachyonConf = conf;
    if (!folder.endsWith(TachyonURI.SEPARATOR)) {
      folder += TachyonURI.SEPARATOR;
    }
    mImagePath = folder + imageFileName;
    mEditLogPath = folder + editLogFileName;
    mEditLog = new EditLog(null, true, 0, mTachyonConf);
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
   * @param startingTransactionId the starting transaction id of the edit log.
   * @throws IOException when the operations fails
   */
  public void createEditLog(long startingTransactionId) throws IOException {
    mEditLog = new EditLog(mEditLogPath, false, startingTransactionId, mTachyonConf);
  }

  /**
   * Create a new image of the Master. It will be created at the mImagePath. If the
   * mStandbyImagePath isn't null, it will rename the mStandbyImagePath to the mImagePath.
   *
   * @param info The Master Info
   * @throws IOException when the operation fails
   */
  public void createImage(MasterInfo info) throws IOException {
    if (mStandbyImagePath == null) {
      Image.create(info, mImagePath);
      EditLog.markUpToDate(mEditLogPath, info);
    } else {
      Image.rename(mStandbyImagePath, mImagePath, info);
    }
  }

  /**
   * Create a new image of the Master to the specified path.
   *
   * @param info The Master Info
   * @param imagePath The path of the image to be created
   * @throws IOException when the operation fails
   */
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
   * @throws IOException when the operation fails
   */
  public long getImageModTimeMs() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mImagePath, mTachyonConf);
    if (!ufs.exists(mImagePath)) {
      return -1;
    }
    return ufs.getModificationTimeMs(mImagePath);
  }

  /**
   * Load edit log.
   *
   * @param info The Master Info.
   * @return The last transaction id.
   * @throws IOException when the operation fails
   */
  public long loadEditLog(MasterInfo info) throws IOException {
    return EditLog.load(info, mEditLogPath, mCurrentLogFileNum);
  }

  /**
   * Load image file.
   *
   * @param info The Master Info.
   * @throws IOException when the operation fails
   */
  public void loadImage(MasterInfo info) throws IOException {
    Image.load(info, mImagePath);
  }

  /**
   * Load one log file of the Master
   *
   * @param info The Master Info
   * @param path The path of the edit log
   * @throws IOException when the operation fails
   */
  public void loadSingleLogFile(MasterInfo info, String path) throws IOException {
    EditLog.loadSingleLog(info, path);
    mCurrentLogFileNum ++;
  }

  /**
   * Changes the max edit log size for testing purposes
   *
   * @param size
   */
  void setMaxLogSize(int size) {
    mEditLog.setMaxLogSize(size);
  }
}
