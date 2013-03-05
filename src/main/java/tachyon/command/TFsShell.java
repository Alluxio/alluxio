
package tachyon.command;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Collections;

import org.apache.thrift.TException;

import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.ClientFileInfo;

import tachyon.CommonUtils;
import tachyon.client.TachyonClient;


public class TFsShell {

  public TFsShell() {}

  private final String TAB = "    ";

  public void printUsage(String cmd) {}
  public void close() {}
  
  public int listStatus(String argv[]) throws FileDoesNotExistException, InvalidPathException, 
      TException {
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
	  for (ClientFileInfo file : files) {
      System.out.print(file.getName() + TAB + CommonUtils.getSizeFromBytes(file.getSizeBytes()));
      System.out.print(TAB + CommonUtils.convertMsToDate(file.getCreationTimeMs()));
      if (file.isInMemory()) {
        System.out.println(TAB + "In Memory");
      } else {
        System.out.println(TAB + "Not In Memory");
      }
	  }
	  return 0;
  }
  
  public int remove(String argv[]) throws FileDoesNotExistException, InvalidPathException, 
      TException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs rm");
      System.out.println("       [rm <path>]");
      return -1;
    }
    String path = argv[1];
	  String folder = Utils.getFilePath(path);
	  TachyonClient tachyonClient = TachyonClient.getClient(Utils.getTachyonMasterAddress(path));
	  tachyonClient.deleteFile(folder);
	  System.out.println(folder + " has been removed");
	  return 0;
  }
  
  /*
  TODO Implement rename

  public int rename(String argv[]) throws FileDoesNotExistException, InvalidPathException,
      TException, IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs mv");
      System.out.println("       [mv <src> <dst>]");
      return -1;
    }
    srcFilePath = argv[1];
    dstFilePath = argv[2];
	  InetSocketAddress srcFileAddr = Utils.getTachyonMasterAddress(srcFilePath);
	  InetSocketAddress dstFileAddr = Utils.getTachyonMasterAddress(dstFilePath);
	  if (!srcFileAddr.getHostName().equals(dstFileAddr.getHostName()) || 
		  srcFileAddr.getPort() != dstFileAddr.getPort()) {
		  throw new IOException("The file system of source and destination must be the same");
	  }
	  String srcFile = Utils.getFilePath(srcFilePath);
	  String dstFile = Utils.getFilePath(dstFilePath);
	  TachyonClient tachyonClient = new TachyonClient(srcFileAddr);
	  tachyonClient.user_renameFile(srcFile, dstFile);
      System.out.println("Renamed " + srcFile + " to " + dstFile);
      return 0;
  }
  */

  public int copyToLocal(String argv[]) throws FileDoesNotExistException, InvalidPathException,
      TException, IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs copyToLocal");
      System.out.println("       [copyToLocal <src> <localdst>]");
      return -1;
    }
    return 0;
  }

  public void printUsage() {
    System.out.println("Usage: java TFsShell");
    System.out.println("       [ls <path>]");
    System.out.println("       [rm <path>]");
    System.out.println("       [copyToLocal <src> <localDst>]");
  }
  
  public static void main(String argv[]) throws TException{
	  System.out.flush();
    TFsShell shell = new TFsShell();
	  int exitCode;
	  try {
	    exitCode = shell.run(argv);
	  } finally {
	    shell.close();
	  }
	  System.exit(exitCode);
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
      }
      if (cmd.equals("rm")) {
    	  exitCode = remove(argv);
      }
      if (cmd.equals("copyToLocal")) {
    	  exitCode = copyToLocal(argv);
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