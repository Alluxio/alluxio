package tachyon.master;

import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.util.CommonUtils;

/**
 * Class that periodically looks for completed edit logs to update metadata of failover nodes.
 */
public class EditLogProcessor implements Runnable {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private Journal mJournal;
  private String mPath;
  private MasterInfo mMasterInfo;
  private int mCurrentLogFileNum = 0;
  private int mLastImageFileNum = 0;
  private long mLoadedImageModTime = 0L;
  private boolean mIsStandby = true;

  public EditLogProcessor(Journal journal, String path, MasterInfo info) {
    mJournal = journal;
    mPath = path;
    mMasterInfo = info;
    try {
      mLoadedImageModTime = mJournal.getImageModTimeMs();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
    LOG.info("Created edit log processor with path " + mPath);
  }

  @Override
  public void run() {
    LOG.info("Edit log processor with path " + mPath + " started.");
    UnderFileSystem ufs = UnderFileSystem.get(mPath);
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
        CommonUtils.runtimeException(e);
      }
    }
    LOG.info("Standy log processor with path " + mPath + " stopped.");
  }

  public void stop() {
    LOG.info("Stopping standby log processor with path " + mPath);
    mIsStandby = false;
  }
}