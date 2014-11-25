package tachyon.command.commands;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.io.Closer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.command.AbstractCommands;
import tachyon.conf.UserConf;

/**
 * Copies a file or directory specified by argv from the local filesystem to the filesystem. Will
 * fail if the path given already exists in the filesystem.
 *
 */
public class CopyFromLocalCommand extends AbstractCommands {
  public static final String NAME = "copyFromLocal";
  public static final String DESCRIPTION =
      "Copies a file or directory specified by argv from the local filesystem to the filesystem.";

  /**
   * Constructor with Closer
   *
   * @param c
   */
  public CopyFromLocalCommand(Closer c) {
    super(c);
  }

  /**
   * Copies a file or directory specified by argv from the local filesystem to the filesystem. Will
   * fail if the path given already exists in the filesystem.
   *
   * @param cmdl  Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int copyFromLocal(CommandLine cmdl) throws IOException, ParseException {
    /*if (cmdl.getOptions()[0].getValue(0) == null || null == cmdl.getOptions()[0].getValue(1) ) {
      System.out.println("Usage: tfs copyFromLocal <src> <remoteDst>");
      return -1;
    }*/
    String srcPath = null;
    TachyonURI dstPath = null;
    try {
      srcPath = cmdl.getOptions()[0].getValue(0);
      dstPath = new TachyonURI(cmdl.getOptions()[0].getValue(1));
    } catch (Exception e) {
      throw new ParseException("Missing second argument for option: copyFromLocal");
    }
    File src = new File(srcPath);
    if (!src.exists()) {
      System.out.println("Local path " + srcPath + " does not exist.");
      return -1;
    }
    TachyonFS tachyonClient = createFS(dstPath);
    int ret = copyPath(src, tachyonClient, dstPath);
    if (ret == 0) {
      System.out.println("Copied " + srcPath + " to " + dstPath);
    }
    return ret;
  }

  private int copyPath(File src, TachyonFS tachyonClient, TachyonURI dstPath) throws IOException {
    if (!src.isDirectory()) {
      int fileId = tachyonClient.createFile(dstPath);
      if (fileId == -1) {
        return -1;
      }
      TachyonFile tFile = tachyonClient.getFile(fileId);
      Closer closer = Closer.create();
      try {
        OutStream os = closer.register(tFile.getOutStream(UserConf.get().DEFAULT_WRITE_TYPE));
        FileInputStream in = closer.register(new FileInputStream(src));
        FileChannel channel = closer.register(in.getChannel());
        ByteBuffer buf = ByteBuffer.allocate(Constants.KB);
        while (channel.read(buf) != -1) {
          buf.flip();
          os.write(buf.array(), 0, buf.limit());
        }
      } finally {
        closer.close();
      }
      return 0;
    } else {
      tachyonClient.mkdir(dstPath);
      for (String file : src.list()) {
        TachyonURI newPath = new TachyonURI(dstPath, new TachyonURI(file));
        File srcFile = new File(src, file);
        if (copyPath(srcFile, tachyonClient, newPath) == -1) {
          return -1;
        }
      }
    }
    return 0;
  }

  @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return copyFromLocal(cmdl);
  }
}