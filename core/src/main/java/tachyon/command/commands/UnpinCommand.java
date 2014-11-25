package tachyon.command.commands;

import java.io.IOException;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * Unpins the given file or folder (recursively unpinning all children if a folder). Pinned files
 * are never evicted from memory, so this method will allow such files to be evicted.
 *
 */
public class UnpinCommand extends AbstractCommands {
  public static final String NAME = "unpin";
  public static final String DESCRIPTION =
            "Unpins the given file or folder.";

   /**
   * Constructor with Closer
   *
   * @param c
   */
  public UnpinCommand(Closer c) {
    super(c);
  }

  @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return unpin(cmdl);
  }
  
  /**
   * Unpins the given file or folder (recursively unpinning all children if a folder). Pinned files
   * are never evicted from memory, so this method will allow such files to be evicted.
   *
   * @param cmdl Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int unpin(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    try {
      tachyonClient.unpinFile(fileId);
      System.out.println("File '" + path + "' was successfully unpinned.");
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("File '" + path + "' could not be unpinned.");
      return -1;
    }
  }
}