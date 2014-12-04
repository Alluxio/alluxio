package tachyon.command;

import java.io.IOException;

import com.google.common.io.Closer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;

/**
 * Manage polymorphism commands in shell
 */
public abstract class AbstractCommands {
  /**
   * Closer
   */
  protected final Closer mCloser;

  /**
   * Constructor with Closer
   * @param c
   */
  public AbstractCommands(Closer c) {
    super();
    mCloser = c;
  }

  /**
   * Executes the command polymorphic behavior
   *
   * @param cmdl  Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public abstract int execute(CommandLine cmdl) throws IOException, ParseException;

  /**
   * Creates a new TachyonFS and registers it with {@link #mCloser}
   */
  protected TachyonFS createFS(final TachyonURI path) throws IOException {
    String qualifiedPath = Utils.validatePath(path.toString());
    return mCloser.register(TachyonFS.get(new TachyonURI(qualifiedPath)));
  }

  protected Closer getCloser() {
    return mCloser;
  }

}