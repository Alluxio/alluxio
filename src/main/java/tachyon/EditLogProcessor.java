package tachyon;

import java.io.IOException;

import org.apache.log4j.Logger;

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
    LOG.info("Created edit log processor!");
  }

  @Override
  public void run() {
    LOG.info("Running edit log processor!");
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
        Thread.sleep(1000);
      } catch (IOException | InterruptedException e) {
        CommonUtils.runtimeException(e);
      }
    }
    LOG.info("Standy log processor stopped.");
  }

  public void stop() {
    LOG.info("Stopping standby log processor");
    mIsStandby = false;
  }
}
