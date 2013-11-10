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
