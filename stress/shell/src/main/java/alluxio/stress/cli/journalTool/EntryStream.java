package alluxio.stress.cli.journalTool;

import java.io.Closeable;
import java.io.IOException;

public abstract class EntryStream implements Closeable {

  protected final String mInputDir;

  public EntryStream(String inputDir) {
    mInputDir = inputDir;
  }

  /**
   * return one journal entry and one step forward
   */
  abstract public void nextEntry();

  /**
   * @return whether next entry exist
   * true if exist
   * false if not exist
   */
  abstract public boolean checkNext();

  @Override
  public void close() throws IOException {

  }
}
