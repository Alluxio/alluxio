package tachyon.command;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Collections;

import org.apache.thrift.TException;

import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.ClientFileInfo;

import tachyon.CommonUtils;
import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;

public class TFsShell {
  public void printUsage(String cmd) {}
  public void close() {}

  public int listStatus(String argv[])
      throws FileDoesNotExistException, InvalidPathException, TException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs ls");
      System.out.println("       [ls <path>]");
      return -1;
    }
    String path = argv[1];
    String folder = Utils.getFilePath(path);
    TachyonClient tachyonClient = TachyonClient.getClient(Utils.getTachyonMasterAddress(path));
    List<ClientFileInfo> files = tachyonClient.listStatus(folder);
    Collections.sort(files);
    int maxLength = 0;
    for (ClientFileInfo file : files) {
      maxLength = Math.max(maxLength, file.getPath().length());
    }
    maxLength = Math.min(maxLength + 3, 100);
    String format = "%-10s%-25s%-15s%-" + maxLength + "s\n";
    for (ClientFileInfo file : files) {
      System.out.format(format, CommonUtils.getSizeFromBytes(file.getSizeBytes()),
          CommonUtils.convertMsToDate(file.getCreationTimeMs()), 
          file.isInMemory() ? "In Memory" : "Not In Memory", file.getPath());
    }
    return 0;
  }

  public int remove(String argv[]) 
      throws FileDoesNotExistException, InvalidPathException, TException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs rm");
      System.out.println("       [rm <path>]");
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
      throws FileDoesNotExistException, InvalidPathException, TException, IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs mv");
      System.out.println("       [mv <src> <dst>]");
      return -1;
    }
    String srcFilePath = argv[1];
    String dstFilePath = argv[2];
	  InetSocketAddress srcFileAddr = Utils.getTachyonMasterAddress(srcFilePath);
	  InetSocketAddress dstFileAddr = Utils.getTachyonMasterAddress(dstFilePath);
	  if (!srcFileAddr.getHostName().equals(dstFileAddr.getHostName()) || 
		  srcFileAddr.getPort() != dstFileAddr.getPort()) {
		  throw new IOException("The file system of source and destination must be the same");
	  }
	  String srcFile = Utils.getFilePath(srcFilePath);
	  String dstFile = Utils.getFilePath(dstFilePath);
	  TachyonClient tachyonClient = TachyonClient.getClient(srcFileAddr);
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
      System.out.println("Usage: tfs copyToLocal");
      System.out.println("       [copyToLocal <src> <localdst>]");
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

    tFile.open("r");
    ByteBuffer buf = tFile.readByteBuffer();
    buf.rewind();
    FileOutputStream out = new FileOutputStream(dst);
    FileChannel channel = out.getChannel();
    while(buf.hasRemaining()) {
      channel.write(buf);
    }
    channel.close();
    out.close();
    System.out.println("Copied " + srcPath + " to " + dstPath);
    return 0;
  }

  public void printUsage() {
    System.out.println("Usage: java TFsShell");
    System.out.println("       [ls <path>]");
    System.out.println("       [rm <path>]");
    System.out.println("       [mv <src> <dst>");
    System.out.println("       [copyToLocal <src> <localDst>]");
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
        exitCode = listStatus(argv);	
      } else if (cmd.equals("rm")) {
        exitCode = remove(argv);
      } else if (cmd.equals("mv")) {
        exitCode = rename(argv);
      } else if (cmd.equals("copyToLocal")) {
        exitCode = copyToLocal(argv);
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
    } finally {
    }

    return exitCode;
  }
}