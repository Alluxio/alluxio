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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;

/**
 * Class that periodically looks for completed edit logs to update metadata of failover nodes.
 */
public class EditLogProcessor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Journal of the master. */
  private final Journal mJournal;
  /** path of the edit logs. */
  private final String mPath;
  private final MasterInfo mMasterInfo;

  private int mCurrentLogFileNum = 0;
  private int mLastImageFileNum = 0;
  private long mLoadedImageModTime = 0L;
  private final TachyonConf mTachyonConf;
  private volatile boolean mIsStandby = true;

  /**
   * Create a new EditLogProcessor.
   *
   * @param journal The journal of the Master
   * @param path The path of the edit logs
   * @param info The Master Info
   * @param tachyonConf The TachyonConf instance.
   */
  public EditLogProcessor(Journal journal, String path, MasterInfo info, TachyonConf tachyonConf) {
    mJournal = journal;
    mPath = path;
    mMasterInfo = info;
    mTachyonConf = tachyonConf;
    try {
      mLoadedImageModTime = mJournal.getImageModTimeMs();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    LOG.info("Created edit log processor with path " + mPath);
  }

  @Override
  public void run() {
    LOG.info("Edit log processor with path " + mPath + " started.");
    UnderFileSystem ufs = UnderFileSystem.get(mPath, mTachyonConf);
    while (mIsStandby) {
      try {
        synchronized (mJournal) {
          long lastImageModTime = mJournal.getImageModTimeMs();
          if (mLoadedImageModTime != lastImageModTime) {
            LOG.info("The last loaded image is out of date. Loading updated image.");
            LOG.info("Loaded image modification time was: " + mLoadedImageModTime);
            LOG.info("Last image mod time was: " + lastImageModTime);
            mJournal.loadImage(mMasterInfo);
            LOG.info("Finished loading new image.");
            mLoadedImageModTime = lastImageModTime;
            mCurrentLogFileNum = 0;
            mLastImageFileNum = 0;
          }
          String path = mPath + "completed/" + mCurrentLogFileNum + ".editLog";
          while (ufs.exists(path)) {
            LOG.info("Found completed log file " + path);
            mJournal.loadSingleLogFile(mMasterInfo, path);
            LOG.info("Finished loading log file " + path);
            mCurrentLogFileNum ++;
            path = mPath + "completed/" + mCurrentLogFileNum + ".editLog";
          }
          LOG.info("Edit log with " + path + " was not found.");
          if (mLastImageFileNum != mCurrentLogFileNum) {
            LOG.info("Last image was updated with log number: " + mLastImageFileNum
                + " writing new image up to log number " + mCurrentLogFileNum);
            mJournal.createImage(mMasterInfo, mPath + mMasterInfo.getMasterAddress().getHostName()
                + mMasterInfo.getMasterAddress().getPort() + "/standby.image");
            LOG.info("Finished creating image");
            mLastImageFileNum = mCurrentLogFileNum;
          }
        }
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    LOG.info("Standby log processor with path " + mPath + " stopped.");
  }

  /**
   * Stop the log processor. Set the stand-by flag false.
   */
  public void stop() {
    LOG.info("Stopping standby log processor with path " + mPath);
    mIsStandby = false;
  }
}
