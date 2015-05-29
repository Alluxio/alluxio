/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.command;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.command.permission.ChmodParser;
import tachyon.conf.TachyonConf;
import tachyon.master.permission.AclUtil;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;

/**
 * Class for handling command line inputs.
 */
public class TFsShell implements Closeable {
  /**
   * Main method, starts a new TFsShell
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    TFsShell shell = new TFsShell(new TachyonConf());
    int ret;
    try {
      ret = shell.run(argv);
    } finally {
      shell.close();
    }
    System.exit(ret);
  }

  private final Closer mCloser;
  private final TachyonConf mTachyonConf;
  private final SimpleDateFormat mDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

  public TFsShell(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mCloser = Closer.create();
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  /**
   * Prints the file's contents to the console.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int cat(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs cat <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);

    if (tFile == null) {
      System.out.println(path + " does not exist.");
      return -1;
    }
    if (tFile.isFile()) {
      InStream is = tFile.getInStream(ReadType.NO_CACHE);
      byte[] buf = new byte[512];
      try {
        int read = is.read(buf);
        while (read != -1) {
          System.out.write(buf, 0, read);
          read = is.read(buf);
        }
      } finally {
        is.close();
      }
      return 0;
    } else {
      System.out.println(path + " is not a file.");
      return -1;
    }
  }

  /**
   * Copies a file or directory specified by argv from the local filesystem to the filesystem. Will
   * fail if the path given already exists in the filesystem.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int copyFromLocal(String[] argv) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs copyFromLocal <src> <remoteDst>");
      return -1;
    }

    String srcPath = argv[1];
    TachyonURI dstPath = new TachyonURI(argv[2]);
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
      TachyonFile tFile = tachyonClient.getFile(dstPath);
      if (tFile != null && tFile.isDirectory()) {
        dstPath = dstPath.join(src.getName());
      }
      int fileId = tachyonClient.createFile(dstPath);
      if (fileId == -1) {
        return -1;
      }
      tFile = tachyonClient.getFile(fileId);
      Closer closer = Closer.create();
      try {
        WriteType writeType =
            mTachyonConf.getEnum(Constants.USER_DEFAULT_WRITE_TYPE, WriteType.CACHE_THROUGH);
        OutStream os = closer.register(tFile.getOutStream(writeType));
        FileInputStream in = closer.register(new FileInputStream(src));
        FileChannel channel = closer.register(in.getChannel());
        ByteBuffer buf = ByteBuffer.allocate(8 * Constants.MB);
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

  /**
   * Copies a file specified by argv from the filesystem to the local filesystem.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
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
      byte[] buf = new byte[64 * Constants.MB];
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

  /**
   * Displays the number of folders and files matching the specified prefix in argv.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int count(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs count <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    try {
      long[] values = countHelper(path);
      String format = "%-25s%-25s%-15s%n";
      System.out.format(format, "File Count", "Folder Count", "Total Bytes");
      System.out.format(format, values[0], values[1], values[2]);
    } catch (FileDoesNotExistException e) {
      System.out.println(e.getMessage() + " does not exist.");
      return -1;
    }
    return 0;
  }

  private long[] countHelper(TachyonURI path) throws FileDoesNotExistException, IOException {
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);
    if (tFile == null) {
      throw new FileDoesNotExistException(path.toString());
    }

    if (tFile.isFile()) {
      return new long[] {1L, 0L, tFile.length()};
    }

    long[] rtn = new long[] {0L, 1L, 0L};

    List<ClientFileInfo> files = tachyonClient.listStatus(path);
    Collections.sort(files);
    for (ClientFileInfo file : files) {
      long[] toAdd = countHelper(new TachyonURI(file.getPath()));
      rtn[0] += toAdd[0];
      rtn[1] += toAdd[1];
      rtn[2] += toAdd[2];
    }
    return rtn;
  }

  /**
   * Displays the file's all blocks info
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int fileinfo(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs fileinfo <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    if (fileId == -1) {
      System.out.println(path + " does not exist.");
      return -1;
    }
    List<ClientBlockInfo> blocks = tachyonClient.getFileBlocks(fileId);
    System.out.println(path + " with file id " + fileId + " has the following blocks: ");
    for (ClientBlockInfo block : blocks) {
      System.out.println(block);
    }
    return 0;
  }

  /**
   * Displays a list of hosts that have the file specified in argv stored.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int location(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs location <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    if (fileId == -1) {
      System.out.println(path + " does not exist.");
      return -1;
    }
    List<String> hosts = tachyonClient.getFile(fileId).getLocationHosts();
    System.out.println(path + " with file id " + fileId + " is on nodes: ");
    for (String host : hosts) {
      System.out.println(host);
    }
    return 0;
  }

  /**
   * Displays information for all directories and files directly under the path specified in argv.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int ls(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs ls <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    List<ClientFileInfo> files = tachyonClient.listStatus(path);
    Collections.sort(files);
    print(files);
    return 0;
  }

  /**
   * Displays information for all directories and files under the path specified in argv
   * recursively.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int lsr(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs lsr <path>");
      return -1;
    }
    List<ClientFileInfo> files = Lists.newArrayList();
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    lsrCore(tachyonClient, path.getPath(), files);
    Collections.sort(files);
    print(files);
    return 0;
  }

  private void lsrCore(TachyonFS client, String path, List<ClientFileInfo> returnFiles)
      throws IOException {
    List<ClientFileInfo> files = client.listStatus(new TachyonURI(path));
    returnFiles.addAll(files);
    for (ClientFileInfo file : files) {
      if (file.isFolder) {
        lsrCore(client, file.path, returnFiles);
      }
    }
  }

  /**
   * Compute column widths and rebuild the format string
   *
   * @param items to find the max field width for each column
   * @return the
   */
  private String adjustColumnWidths(List<ClientFileInfo> items) {
    int maxLen = Integer.MIN_VALUE;
    int maxOwner = Integer.MIN_VALUE;
    int maxGroup = Integer.MIN_VALUE;

    for (ClientFileInfo item : items) {
      maxLen = maxLength(maxLen, item.length);
      maxOwner = maxLength(maxOwner, item.owner);
      maxGroup = maxLength(maxGroup, item.group);
    }

    StringBuilder fmt = new StringBuilder();
    fmt.append("%s%s "); // permission string
    fmt.append((maxOwner > 0) ? "%-" + maxOwner + "s " : "%s"); // owner
    fmt.append((maxGroup > 0) ? "%-" + maxGroup + "s " : "%s"); // group
    fmt.append((maxLen > 0) ? "%-" + maxLen + "s " : "%s"); // lenght
    fmt.append("%s "); // time
    fmt.append("%-12s "); // memory or not
    fmt.append("%s"); // path
    return fmt.toString();
  }

  private int maxLength(int n, Object value) {
    return Math.max(n, (value != null) ? String.valueOf(value).length() : 0);
  }

  private boolean inMemoryFile(ClientFileInfo file) {
    if (!file.isFolder && (file.inMemoryPercentage == 100)) {
      return true;
    }
    return false;
  }

  private void print(List<ClientFileInfo> items) throws IOException {
    String lineFormat = adjustColumnWidths(items);
    for (ClientFileInfo item : items) {
      String line =
          String.format(lineFormat, (item.isFolder ? "d" : "-"), AclUtil
              .formatPermission((short) item.permission), item.owner, item.group, String
              .valueOf((item.length)), mDateFormat.format(new Date(item.lastModificationTimeMs)),
              item.isFolder ? "" : (inMemoryFile(item) ? "InMemory" : "Not InMemory"), item.path);
      System.out.println(line);
    }
  }

  /**
   * Creates a new directory specified by the path in argv, including any parent folders that are
   * required. This method fails if a directory or file with the same path already exists.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int mkdir(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs mkdir <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.mkdir(path)) {
      System.out.println("Successfully created directory " + path);
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Pins the given file or folder (recursively pinning all children if a folder). Pinned files are
   * never evicted from memory.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int pin(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs pin <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);

    try {
      tachyonClient.pinFile(fileId);
      System.out.println("File '" + path + "' was successfully pinned.");
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("File '" + path + "' could not be pinned.");
      return -1;
    }
  }

  /**
   * Method which prints the method to use all the commands.
   */
  public void printUsage() {
    System.out.println("Usage: java TFsShell");
    System.out.println("       [cat <path>]");
    System.out.println("       [count <path>]");
    System.out.println("       [ls <path>]");
    System.out.println("       [lsr <path>]");
    System.out.println("       [mkdir <path>]");
    System.out.println("       [rm <path>]");
    System.out.println("       [rmr <path>]");
    System.out.println("       [tail <path>]");
    System.out.println("       [touch <path>]");
    System.out.println("       [mv <src> <dst>]");
    System.out.println("       [copyFromLocal <src> <remoteDst>]");
    System.out.println("       [copyToLocal <src> <localDst>]");
    System.out.println("       [fileinfo <path>]");
    System.out.println("       [location <path>]");
    System.out.println("       [report <path>]");
    System.out.println("       [request <tachyonaddress> <dependencyId>]");
    System.out.println("       [pin <path>]");
    System.out.println("       [unpin <path>]");
    System.out.println("       [free <file path|folder path>]");
    System.out.println("       [chmod <mode> <file path|folder path>]");
    System.out.println("       [chmodr <mode> <file path|folder path>]");
    System.out.println("       [chown <owner|owner:group> <file path|folder path>]");
    System.out.println("       [chownr <owner|owner:group> <file path|folder path>]");
    System.out.println("       [chgrp <group> <file path|folder path>]");
    System.out.println("       [chgrpr <group> <file path|folder path>]");
  }

  /**
   * Renames a file or directory specified by argv. Will fail if the new path name already exists.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int rename(String[] argv) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs mv <src> <dst>");
      return -1;
    }
    TachyonURI srcPath = new TachyonURI(argv[1]);
    TachyonURI dstPath = new TachyonURI(argv[2]);
    TachyonFS tachyonClient = createFS(srcPath);
    if (tachyonClient.rename(srcPath, dstPath)) {
      System.out.println("Renamed " + srcPath + " to " + dstPath);
      return 0;
    } else {
      return -1;
    }
  }

  public int report(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs report <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    tachyonClient.reportLostFile(fileId);
    System.out.println(path + " with file id " + fileId + " has reported been report lost.");
    return 0;
  }

  public int request(String[] argv) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs request <tachyonaddress> <dependencyId>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    int depId = Integer.parseInt(argv[2]);
    TachyonFS tachyonClient = createFS(path);
    tachyonClient.requestFilesInDependency(depId);
    System.out.println("Dependency with ID " + depId + " has been requested.");
    return 0;
  }

  /**
   * Removes the file specified by argv.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int rm(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs rm <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);
    if (tFile != null && tFile.isDirectory()) {
      System.out.println("can't remove a directory, please try rmr <path>");
      return -1;
    }

    if (tachyonClient.delete(path, false)) {
      System.out.println(path + " has been removed");
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Removes the file or directory specified by argv. Will remove all files and directories in the
   * directory if a directory is specified.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int rmr(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs rmr <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.delete(path, true)) {
      System.out.println(path + " has been removed");
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Changes the mode of a file or a directory specified by argv.
   * 
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int chmod(String[] argv) throws IOException {
    return chmodCore(argv, false);
  }

  /**
   * Changes the mode of a file or a directory specified by argv recursively.
   * 
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int chmodr(String[] argv) throws IOException {
    return chmodCore(argv, true);
  }

  private int chmodCore(String[] argv, boolean recursive) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: chmod <mode> <file path|folder path>");
      return -1;
    }
    String modeStr = argv[1];
    ChmodParser pp = new ChmodParser(modeStr);
    TachyonURI path = new TachyonURI(argv[2]);
    TachyonFS tachyonClient = createFS(path);
    ClientFileInfo status = tachyonClient.getFileStatus(-1, path);
    short newPermission = pp.applyNewPermission(status);
    int fileId = tachyonClient.getFileId(path);
    tachyonClient.setPermission(fileId, newPermission, recursive);
    return 0;
  }

  /**
   * Changes the owner of a file or a directory specified by argv.
   * 
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int chown(String[] argv) throws IOException {
    return chownCore(argv, false);
  }

  /**
   * Changes the owner of a file or a directory specified by argv recursively.
   * 
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int chownr(String[] argv) throws IOException {
    return chownCore(argv, true);
  }

  private int chownCore(String[] argv, boolean recursive) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: chown <owner|owner:group> <file path|folder path>");
      return -1;
    }
    String owner = "";
    String group = null;
    if (argv[1].contains(":")) {
      String[] user = argv[1].split(":");
      owner = user[0];
      group = user[1];
    } else {
      owner = argv[1];
    }
    TachyonURI path = new TachyonURI(argv[2]);
    TachyonFS tachyonClient = createFS(path);
    tachyonClient.setOwner(path, owner, group, recursive);
    return 0;
  }

  /**
   * Changes the group of a file or a directory specified by argv.
   * 
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int chgrp(String[] argv) throws IOException {
    return chgrpCore(argv, false);
  }

  /**
   * Changes the group of a file or a directory specified by argv recursively.
   * 
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int chgrpr(String[] argv) throws IOException {
    return chgrpCore(argv, true);
  }

  private int chgrpCore(String[] argv, boolean recursive) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: chgrp <group> <file path|folder path>");
      return -1;
    }
    String group = argv[1];
    TachyonURI path = new TachyonURI(argv[2]);
    TachyonFS tachyonClient = createFS(path);
    tachyonClient.setOwner(path, null, group, recursive);
    return 0;
  }

  /**
   * Method which determines how to handle the user's request, will display usage help to the user
   * if command format is incorrect.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred
   */
  public int run(String[] argv) {
    if (argv.length == 0) {
      printUsage();
      return -1;
    }

    String cmd = argv[0];
    int exitCode = -1;
    try {
      if (cmd.equals("cat")) {
        exitCode = cat(argv);
      } else if (cmd.equals("count")) {
        exitCode = count(argv);
      } else if (cmd.equals("ls")) {
        exitCode = ls(argv);
      } else if (cmd.equals("lsr")) {
        exitCode = lsr(argv);
      } else if (cmd.equals("mkdir")) {
        exitCode = mkdir(argv);
      } else if (cmd.equals("rm")) {
        exitCode = rm(argv);
      } else if (cmd.equals("rmr")) {
        exitCode = rmr(argv);
      } else if (cmd.equals("tail")) {
        exitCode = tail(argv);
      } else if (cmd.equals("mv")) {
        exitCode = rename(argv);
      } else if (cmd.equals("touch")) {
        exitCode = touch(argv);
      } else if (cmd.equals("copyFromLocal")) {
        exitCode = copyFromLocal(argv);
      } else if (cmd.equals("copyToLocal")) {
        exitCode = copyToLocal(argv);
      } else if (cmd.equals("fileinfo")) {
        exitCode = fileinfo(argv);
      } else if (cmd.equals("location")) {
        exitCode = location(argv);
      } else if (cmd.equals("report")) {
        exitCode = report(argv);
      } else if (cmd.equals("request")) {
        exitCode = request(argv);
      } else if (cmd.equals("pin")) {
        exitCode = pin(argv);
      } else if (cmd.equals("unpin")) {
        exitCode = unpin(argv);
      } else if (cmd.equals("free")) {
        exitCode = free(argv);
      } else if (cmd.equals("chmod")) {
        exitCode = chmod(argv);
      } else if (cmd.equals("chmodr")) {
        exitCode = chmodr(argv);
      } else if (cmd.equals("chown")) {
        exitCode = chown(argv);
      } else if (cmd.equals("chownr")) {
        exitCode = chownr(argv);
      } else if (cmd.equals("chgrp")) {
        exitCode = chgrp(argv);
      } else if (cmd.equals("chgrpr")) {
        exitCode = chgrpr(argv);
      } else {
        printUsage();
        return -1;
      }
    } catch (IOException ioe) {
      System.out.println(ioe.getMessage());
    }

    return exitCode;
  }

  /**
   * Prints the file's last 1KB of contents to the console.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.f
   * @throws IOException
   */
  public int tail(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs tail <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);

    if (tFile == null) {
      System.out.println(path + " does not exist.");
      return -1;
    }
    if (tFile.isFile()) {
      InStream is = tFile.getInStream(ReadType.NO_CACHE);
      try {
        byte[] buf = new byte[Constants.KB];
        long bytesToRead = 0L;
        if (tFile.length() > Constants.KB) {
          bytesToRead = Constants.KB;
        } else {
          bytesToRead = tFile.length();
        }
        is.skip(tFile.length() - bytesToRead);
        int read = is.read(buf);
        System.out.write(buf, 0, read);
        return 0;
      } finally {
        is.close();
      }
    } else {
      System.out.println(path + " is not a file.");
      return -1;
    }
  }

  /**
   * Creates a 0 byte file specified by argv.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command if successful, -1 if an error occurred.
   * @throws IOException
   */
  public int touch(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs touch <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(tachyonClient.createFile(path));
    OutputStream out = tFile.getOutStream(WriteType.THROUGH);
    out.close();
    System.out.println(path + " has been created");
    return 0;
  }

  /**
   * Unpins the given file or folder (recursively unpinning all children if a folder). Pinned files
   * are never evicted from memory, so this method will allow such files to be evicted.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int unpin(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs unpin <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
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

  /**
   * Free the file or Folder from tachyon in-memory specified by argv
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command if successful, -1 if an error occurred.
   * @throws IOException
   */
  public int free(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs free <file path | folder path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.freepath(path, true)) {
      System.out.println(path + " was successfully freed from memory.");
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Creates a new TachyonFS and registers it with {@link #mCloser}
   */
  private TachyonFS createFS(final TachyonURI path) throws IOException {
    String qualifiedPath = Utils.validatePath(path.toString(), mTachyonConf);
    TachyonFS tachyonFS = TachyonFS.get(new TachyonURI(qualifiedPath), mTachyonConf);
    return mCloser.register(tachyonFS);
  }
}
