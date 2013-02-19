package tachyon.command;

import org.apache.thrift.TException;

import tachyon.MasterClient;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

public class Rm {
  public static void main(String[] args)
      throws FileDoesNotExistException, InvalidPathException, TException {
    if (args.length != 1) {
      System.out.println("Usage: tachyon\n [-rm <path>]");
      System.exit(-1);
    }

    System.out.println(args[0]);
    String folder = Utils.getDatasetName(args[0]);
    MasterClient masterClient = new MasterClient(Utils.getTachyonMasterAddress(args[0]));
    masterClient.open();
    masterClient.user_delete(folder);
    System.out.println(folder + " has been removed: ");
  }
}