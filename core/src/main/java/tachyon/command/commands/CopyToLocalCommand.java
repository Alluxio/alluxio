package tachyon.command.commands;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.google.common.io.Closer;

import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.command.AbstractCommands;

/**
 * Copies a file specified by argv from the filesystem to the local filesystem.
 *
 */
public class CopyToLocalCommand extends AbstractCommands {
  /**
   * Copies a file specified by argv from the filesystem to the local filesystem.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int copyToLocal(String[] argv) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs copyToLocal <src> <localdst>");
      return -1;
    }

    TachyonURI srcPath = new TachyonURI(argv[1]);
    String dstPath = argv[2];
    File dst = new File(dstPath);
    TachyonFS tachyonClient = createFS(srcPath);
    TachyonFile tFile = tachyonClient.getFile(srcPath);

    // tachyonClient.getFile() catches FileDoesNotExist exceptions and returns null
    if (tFile == null) {
      throw new IOException(srcPath.toString());
    }

    Closer closer = Closer.create();
    try {
      InStream is = closer.register(tFile.getInStream(ReadType.NO_CACHE));
      FileOutputStream out = closer.register(new FileOutputStream(dst));
      byte[] buf = new byte[512];
      int t = is.read(buf);
      while (t != -1) {
        out.write(buf, 0, t);
        t = is.read(buf);
      }
      System.out.println("Copied " + srcPath + " to " + dstPath);
      return 0;
    } finally {
      closer.close();
    }
  }

  @Override
  public int execute(String[] argv) throws IOException {
    return copyToLocal(argv);
  }
}