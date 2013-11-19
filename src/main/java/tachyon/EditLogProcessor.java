package tachyon;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Class that periodically looks for completed edit logs to update metadata of failover nodes.
 */

public class EditLogProcessor implements Runnable {

  private Journal mJournal;
  private String mPath;
  private MasterInfo mMasterInfo;
  private int mCurrentLogNum = 0;
  private int mLastImageNum = 0;
  private boolean mIsStandby = true;
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  public EditLogProcessor(Journal journal, String path, MasterInfo info) {
    mJournal = journal;
    mPath = path;
    mMasterInfo = info;
    LOG.info("Created edit log processor with path " + mPath);
  }

  @Override
  public void run() {
    LOG.info("Edit log processor with path " + mPath + " started.");
    UnderFileSystem ufs = UnderFileSystem.get(mPath);
    while (mIsStandby) {
      try {
        synchronized(mJournal) {
          String path = mPath + "completed/" + mCurrentLogNum + ".editLog";
          if (ufs.exists(path)) {
            LOG.info("Found completed log file " + path);
            mJournal.LoadSingleLog(mMasterInfo, path);
            LOG.info("Finished loading log file " + path);
            mCurrentLogNum ++;
          } else {
            LOG.info("Failed to find path " + path);
            if (mLastImageNum != mCurrentLogNum) {
              LOG.info("Last image was updated with log number: " + mLastImageNum + " writing " +
              		" new image up to log number " + mCurrentLogNum);
              mJournal.createImage(mMasterInfo, mPath + mMasterInfo.getMasterAddress().getHostName()
                  + mMasterInfo.getMasterAddress().getPort() + "/standby.image");
              LOG.info("Finished creating image");
              mLastImageNum = mCurrentLogNum;
            }
          }
        }
        CommonUtils.sleepMs(LOG, 1000);
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
