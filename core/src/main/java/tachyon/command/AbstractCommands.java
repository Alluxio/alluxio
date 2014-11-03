package tachyon.command;

import java.io.IOException;

import com.google.common.io.Closer;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;

/**
 * Manage polymorphism commands in shell
 */
public abstract class AbstractCommands {

  /**
   *
   */
  private Closer mCloser;

  /**
   *
   */
  public AbstractCommands() {
    super();
  }


  /**
   * Executes the command polymorphic behavior
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public abstract int execute(String[] argv) throws IOException;

  /**
   * Creates a new TachyonFS and registers it with {@link #mCloser}
   */
  protected TachyonFS createFS(final TachyonURI path) throws IOException {
    String qualifiedPath = Utils.validatePath(path.toString());
    return mCloser.register(TachyonFS.get(new TachyonURI(qualifiedPath)));
  }

    public Closer getmCloser() {
        return mCloser;
    }

    public void setmCloser(Closer mCloser) {
        this.mCloser = mCloser;
    }
}
