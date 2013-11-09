package tachyon;

import java.io.IOException;

/**
 * The Journal manages Tachyon image and journal log.
 */
public class Journal {
  private EditLog mEditLog = new EditLog(null, true, 0);

  private int mCurrentLogNum = 0;
  private String mImagePath;
  private String mStandbyImagePath = "";
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
    return EditLog.load(info, mEditLogPath, mCurrentLogNum);
  }

  public void LoadSingleLog(MasterInfo info, String path) throws IOException {
    EditLog.loadSingleLog(info, path);
    mCurrentLogNum ++;
  }

  public void createImage(MasterInfo info) throws IOException {
    if (!EditLog.getIsBackUpCurrentLog() || mStandbyImagePath == "") {
      Image.create(info, mImagePath);
    } else {
      Image.rename(mStandbyImagePath, mImagePath);
    }
  }

  public void createImage(MasterInfo info, String imagePath) throws IOException {
    Image.create(info, imagePath);
    mStandbyImagePath = imagePath;
  }

  public void createEditLog(long transactionId) throws IOException {
    EditLog.deleteCompletedLogs(mEditLogPath);
    mEditLog = new EditLog(mEditLogPath, false, transactionId);
  }

  public EditLog getEditLog() {
    return mEditLog;
  }
}
