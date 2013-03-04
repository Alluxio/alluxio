
package tachyon.command;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.thrift.TException;

import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

import tachyon.MasterClient;


public class TFsShell {

  public TFsShell() {}


  public void printUsage(String cmd) {}
  public void close() {}
  
  public int listStatus(String path) throws FileDoesNotExistException, InvalidPathException, 
      TException {
	String folder = Utils.getFilePath(path);
	MasterClient masterClient = new MasterClient(Utils.getTachyonMasterAddress(path));
	masterClient.open();
	List<String> files = masterClient.ls(folder);
	for (String file : files) {
	  String[] split = file.split("/");
      System.out.println(split[split.length-1]);
	}
	return 0;
  }
  
  public int remove(String path) throws FileDoesNotExistException, InvalidPathException, 
      TException {
	String folder = Utils.getFilePath(path);
	MasterClient masterClient = new MasterClient(Utils.getTachyonMasterAddress(path));
	masterClient.open();
	masterClient.user_delete(folder);
	System.out.println(folder + " has been removed: ");
	return 0;
  }
  
  public int rename(String srcFilePath, String dstFilePath) throws FileDoesNotExistException,
      InvalidPathException, TException, IOException {
	  InetSocketAddress srcFileAddr = Utils.getTachyonMasterAddress(srcFilePath);
	  InetSocketAddress dstFileAddr = Utils.getTachyonMasterAddress(dstFilePath);
	  if (!srcFileAddr.getHostName().equals(dstFileAddr.getHostName()) || 
		  srcFileAddr.getPort() != dstFileAddr.getPort()) {
		  throw new IOException("The file system of source and destination must be the same");
	  }
	  String srcFile = Utils.getFilePath(srcFilePath);
	  String dstFile = Utils.getFilePath(dstFilePath);
	  MasterClient masterClient = new MasterClient(srcFileAddr);
	  masterClient.open();
	  masterClient.user_renameFile(srcFile, dstFile);
      System.out.println("Renamed " + srcFile + " to " + dstFile);
      return 0;
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
    String cmd = argv[0];
    int exitCode = -1;
    try {
      if (cmd.equals("ls")) {
        exitCode = listStatus(argv[1]);	
      }
      if (cmd.equals("rm")) {
    	exitCode = remove(argv[1]);
      }
      if (cmd.equals("mv")) {
    	exitCode = rename(argv[1], argv[2]);
      }
    } catch (InvalidPathException ipe) {
      System.out.println(ipe.getMessage());
    } catch (FileDoesNotExistException fdne) {
      System.out.println(fdne.getMessage());
    } catch (IOException ioe) {
      System.out.println(ioe.getMessage());
    } finally {
    }
    
    return exitCode;
  }

}