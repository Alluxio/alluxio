package tachyon;

import java.io.IOException;

/**
 * The Journal manages Tachyon image and journal log.
 */
public class Journal {
  private static final String EDIT_LOG_FILE = "log.data";
  private static final String IMAGE_FILE = "image.data";

  private final MasterInfo MASTER_INFO;

  private EditLog mEditLog;

  private String mImagePath;
  private String mEditLogPath;

  public Journal(String folder, MasterInfo masterInfo) throws IOException {
    MASTER_INFO = masterInfo;

    mEditLogPath = folder + EDIT_LOG_FILE;
    mImagePath = folder + IMAGE_FILE;

    Image.load(MASTER_INFO, mImagePath);
    EditLog.load(MASTER_INFO, mEditLogPath);

    Image.create(MASTER_INFO, mImagePath);
  }

  public EditLog getEditLog() {
    return mEditLog;
  }
}
