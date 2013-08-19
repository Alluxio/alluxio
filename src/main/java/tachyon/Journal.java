package tachyon;

import java.io.IOException;

/**
 * The Journal manages Tachyon image and journal log.
 */
public class Journal {
  static final String EDIT_LOG_FILE = "log.data";
  static final String IMAGE_FILE = "image.data";

  private EditLog mEditLog = new EditLog(null, true, 0);

  private String mImagePath;
  private String mEditLogPath;

  public Journal(String folder) throws IOException {
    mEditLogPath = folder + EDIT_LOG_FILE;
    mImagePath = folder + IMAGE_FILE;
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
}
