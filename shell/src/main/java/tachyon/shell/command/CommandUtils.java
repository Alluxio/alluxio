package tachyon.shell.command;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.SetStateOptions;
import tachyon.client.lineage.TachyonLineage;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.LineageInfo;
import tachyon.util.FormatUtils;

/**
 * Common util methods for executing commands.
 */
public class CommandUtils {

  private CommandUtils() {
    // Not intended for instantiation.
  }

  /**
   * Set a new TTL value or unset an existing TTL value for file at path.
   *
   * @param path the file path
   * @param ttlMs the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, irrespective of
   *        whether the file is pinned; {@link Constants#NO_TTL} means to unset the TTL value
   * @throws IOException when failing to set/unset the TTL
   */
  public static void setTTL(TachyonFileSystem tfs, TachyonURI path, long ttlMs) throws IOException {
    try {
      TachyonFile fd = tfs.open(path);
      SetStateOptions options = new SetStateOptions.Builder().setTTL(ttlMs).build();
      tfs.setState(fd, options);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Displays information for all directories and files directly under the path specified in argv.
   *
   * @param tfs The TachyonFileSystem client
   * @param path The TachyonURI path as the input of the command
   * @param recursive Whether list the path recursively
   * @throws IOException
   */
  public static void ls(TachyonFileSystem tfs, TachyonURI path, boolean recursive)
      throws IOException {
    List<FileInfo> files = listStatusSortedByIncreasingCreationTime(tfs, path);
    String format = "%-10s%-25s%-15s%-15s%-5s%n";
    for (FileInfo file : files) {
      String inMemory = "";
      if (!file.isFolder) {
        if (100 == file.inMemoryPercentage) {
          inMemory = "In Memory";
        } else {
          inMemory = "Not In Memory";
        }
      }
      System.out.format(format, FormatUtils.getSizeFromBytes(file.getLength()),
          convertMsToDate(file.getCreationTimeMs()), inMemory, file.getUserName(),
          file.getPath());
      if (recursive && file.isFolder) {
        ls(tfs, new TachyonURI(path.getScheme(), path.getAuthority(), file.getPath()), true);
      }
    }
  }

  /**
   * Convert a millisecond number to a formatted date String
   * @param millis a long millisecond number
   * @return formatted date String
   */
  public static String convertMsToDate(long millis) {
    DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss:SSS");
    return formatter.format(new Date(millis));
  }

  public static void listLineages() throws IOException {
    TachyonLineage tl = TachyonLineage.get();
    List<LineageInfo> infos = tl.getLineageInfoList();
    for (LineageInfo info : infos) {
      System.out.println(info);
    }
  }

  /**
   * Set pin state for the input path
   *
   * @param tfs The TachyonFileSystem client
   * @param path The TachyonURI path as the input of the command
   * @param pinned the state to be set
   * @throws IOException
   */
  public static void setPinned(TachyonFileSystem tfs, TachyonURI path, boolean pinned)
      throws IOException {
    try {
      TachyonFile fd = tfs.open(path);
      SetStateOptions options = new SetStateOptions.Builder().setPinned(pinned).build();
      tfs.setState(fd, options);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  private static List<FileInfo> listStatusSortedByIncreasingCreationTime(TachyonFileSystem tfs,
      TachyonURI path) throws IOException {
    List<FileInfo> files = null;
    try {
      TachyonFile fd = tfs.open(path);
      files = tfs.listStatus(fd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    Collections.sort(files, new Comparator<FileInfo>() {
      @Override
      public int compare(FileInfo fileInfo, FileInfo fileInfo2) {
        long t1 = fileInfo.creationTimeMs;
        long t2 = fileInfo2.creationTimeMs;
        if (t1 < t2) {
          return -1;
        }
        if (t1 == t2) {
          return 0;
        }
        return 1;
      }
    });
    return files;
  }
}
