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
  private int mCurrentLogFileNum = 0;
  private int mLastImageFileNum = 0;
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
          String path = mPath + "completed/" + mCurrentLogFileNum + ".editLog";
          if (ufs.exists(path)) {
            LOG.info("Found completed log file " + path);
            mJournal.loadSingleLogFile(mMasterInfo, path);
            LOG.info("Finished loading log file " + path);
            mCurrentLogFileNum ++;
          } else {
            LOG.info("Failed to find path " + path);
            if (mLastImageFileNum != mCurrentLogFileNum) {
              LOG.info("Last image was updated with log number: " + mLastImageFileNum + " writing "
              		+ " new image up to log number " + mCurrentLogFileNum);
              mJournal.createImage(mMasterInfo, mPath + mMasterInfo.getMasterAddress().getHostName()
                  + mMasterInfo.getMasterAddress().getPort() + "/standby.image");
              LOG.info("Finished creating image");
              mLastImageFileNum = mCurrentLogFileNum;
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
