package tachyon;

import java.io.IOException;

/**
 * The Journal manages Tachyon image and journal log.
 */
public class Journal {
  static final String EDIT_LOG_FILE = "log.data";
  static final String IMAGE_FILE = "image.data";

  private EditLog mEditLog = new EditLog(null, true);

  private String mImagePath;
  private String mEditLogPath;

  public Journal(String folder) throws IOException {
    mEditLogPath = folder + EDIT_LOG_FILE;
    mImagePath = folder + IMAGE_FILE;
  }

  public void loadImage(MasterInfo info) throws IOException {
    Image.load(info, mImagePath);
  }

  public void loadEditLog(MasterInfo info) throws IOException {
    EditLog.load(info, mEditLogPath);
  }

  public void createImage(MasterInfo info) throws IOException {
    Image.create(info, mImagePath);
  }
  
  public void createEditLog() throws IOException {
    mEditLog = new EditLog(mEditLogPath, false);
  }

  public EditLog getEditLog() {
    return mEditLog;
  }
}
