package tachyon.command;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Collections;

import org.apache.thrift.TException;

import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.ClientFileInfo;

import tachyon.CommonUtils;
import tachyon.client.OpType;
import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;

public class TFsShell {
  public void printUsage(String cmd) {}
  public void close() {}

  public int ls(String argv[])
      throws FileDoesNotExistException, InvalidPathException, TException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs ls <path>");
      return -1;
    }
    String path = argv[1];
    String folder = Utils.getFilePath(path);
    TachyonClient tachyonClient = TachyonClient.getClient(Utils.getTachyonMasterAddress(path));
    List<ClientFileInfo> files = tachyonClient.listStatus(folder);
    Collections.sort(files);
    String format = "%-10s%-25s%-15s%-5s\n";
    for (ClientFileInfo file : files) {
      System.out.format(format, CommonUtils.getSizeFromBytes(file.getSizeBytes()),
          CommonUtils.convertMsToDate(file.getCreationTimeMs()), 
          file.isInMemory() ? "In Memory" : "Not In Memory", file.getPath());
    }
    return 0;
  }

  public int mkdir(String argv[]) throws InvalidPathException, FileAlreadyExistException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs mkdir <path>");
      return -1;
    }
    String path = argv[1];
    String folder = Utils.getFilePath(path);
    TachyonClient tachyonClient = TachyonClient.getClient(Utils.getTachyonMasterAddress(path));
    if (tachyonClient.mkdir(folder) != -1) {
      System.out.println("Successfully created directory " + folder);
      return 0;
    } else {
      return -1;
    }
  }

  public int rm(String argv[]) 
      throws FileDoesNotExistException, InvalidPathException, TException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs rm <path>");
      return -1;
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonClient tachyonClient = TachyonClient.getClient(Utils.getTachyonMasterAddress(path));
    if (tachyonClient.deleteFile(file)) {
      System.out.println(file + " has been removed");
      return 0;
    } else {
      return -1;
    }
  }

  public int rename(String argv[]) 
      throws FileDoesNotExistException, InvalidPathException, TException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs mv <src> <dst>");
      return -1;
    }
    String srcPath = argv[1];
    String dstPath = argv[2];
    InetSocketAddress srcMasterAddr = Utils.getTachyonMasterAddress(srcPath);
    InetSocketAddress dstMasterAddr = Utils.getTachyonMasterAddress(dstPath);
    if (!srcMasterAddr.getHostName().equals(dstMasterAddr.getHostName()) || 
        srcMasterAddr.getPort() != dstMasterAddr.getPort()) {
      throw new InvalidPathException("The file system of source and destination must be the same");
    }
    String srcFile = Utils.getFilePath(srcPath);
    String dstFile = Utils.getFilePath(dstPath);
    TachyonClient tachyonClient = TachyonClient.getClient(srcMasterAddr);
    if (tachyonClient.renameFile(srcFile, dstFile)) {
      System.out.println("Renamed " + srcFile + " to " + dstFile);
      return 0;
    } else {
      return -1;
    }
  }

  public int copyToLocal(String argv[])
      throws FileDoesNotExistException, InvalidPathException, TException, IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs copyToLocal <src> <localdst>");
      return -1;
    }

    String srcPath = argv[1];
    String dstPath = argv[2];
    String folder = Utils.getFilePath(srcPath);
    File dst = new File(dstPath);
    TachyonClient tachyonClient = TachyonClient.getClient(Utils.getTachyonMasterAddress(srcPath));
    TachyonFile tFile = tachyonClient.getFile(folder);

    // tachyonClient.getFile() catches FileDoesNotExist exceptions and returns null
    if (tFile == null) {
      throw new FileDoesNotExistException(folder);
    }

    tFile.open(OpType.READ_TRY_CACHE);
    ByteBuffer buf = tFile.readByteBuffer();
    FileOutputStream out = new FileOutputStream(dst);
    FileChannel channel = out.getChannel();
    while (buf.hasRemaining()) {
      channel.write(buf);
    }
    channel.close();
    out.close();
    System.out.println("Copied " + srcPath + " to " + dstPath);
    return 0;
  }

  public int location(String argv[]) 
      throws FileDoesNotExistException, InvalidPathException, IOException, TException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs location <path>");
      return -1;
    }
    String path = argv[1];
    String file = Utils.getFilePath(path);
    TachyonClient tachyonClient = TachyonClient.getClient(Utils.getTachyonMasterAddress(path));
    int fileId = tachyonClient.getFileId(file);
    List<String> hosts = tachyonClient.getFileHosts(fileId);
    System.out.println(file + " with file id " + fileId + " are on nodes: ");
    for (String host: hosts) {
      System.out.println(host);
    }
    return 0;
  }

  public int copyFromLocal(String argv[]) 
      throws FileNotFoundException, InvalidPathException, IOException, FileAlreadyExistException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs copyFromLocal <src> <remoteDst>");
      return -1;
    }

    String srcPath = argv[1];
    String dstPath = argv[2];
    String dstFile = Utils.getFilePath(dstPath);
    File src = new File(srcPath);
    TachyonClient tachyonClient = TachyonClient.getClient(Utils.getTachyonMasterAddress(dstPath));
    int fileId = tachyonClient.createFile(dstFile);
    if (fileId == -1) {
      return -1;
    }
    TachyonFile tFile = tachyonClient.getFile(fileId);
    tFile.open(OpType.WRITE_THROUGH);
    FileInputStream in = new FileInputStream(src);
    FileChannel channel = in.getChannel();
    ByteBuffer buf = ByteBuffer.allocate(1024);
    while (channel.read(buf) != -1) {
      buf.flip();
      tFile.append(buf);
    }
    tFile.close();
    channel.close();
    in.close();
    System.out.println("Copied " + srcPath + " to " + dstPath);
    return 0;
  }

  public void printUsage() {
    System.out.println("Usage: java TFsShell");
    System.out.println("       [ls <path>]");
    System.out.println("       [mkdir <path>]");
    System.out.println("       [rm <path>]");
    System.out.println("       [mv <src> <dst>");
    System.out.println("       [copyFromLocal <src> <remoteDst>]");
    System.out.println("       [copyToLocal <src> <localDst>]");
    System.out.println("       [location <path>]");
  }

  public static void main(String argv[]) throws TException{
    TFsShell shell = new TFsShell();
    System.exit(shell.run(argv));
  }

  public int run(String argv[]) throws TException {
    if (argv.length == 0) {
      printUsage();
      return -1;
    }

    String cmd = argv[0];
    int exitCode = -1;
    try {
      if (cmd.equals("ls")) {
        exitCode = ls(argv);
      } else if (cmd.equals("mkdir")) {
        exitCode = mkdir(argv);
      } else if (cmd.equals("rm")) {
        exitCode = rm(argv);
      } else if (cmd.equals("mv")) {
        exitCode = rename(argv);
      } else if (cmd.equals("copyFromLocal")) {
        exitCode = copyFromLocal(argv);
      } else if (cmd.equals("copyToLocal")) {
        exitCode = copyToLocal(argv);
      } else if (cmd.equals("location")) {
        exitCode = location(argv);
      } else {
        printUsage();
        return -1;
      }
    } catch (InvalidPathException ipe) {
      System.out.println("Invalid Path: " + ipe.getMessage());
    } catch (FileDoesNotExistException fdne) {
      System.out.println("File Does Not Exist: " + fdne.getMessage());
    } catch (IOException ioe) {
      System.out.println(ioe.getMessage());
    } catch (FileAlreadyExistException faee) {
      System.out.println(faee.getMessage());
    } finally {
    }

    return exitCode;
  }
}