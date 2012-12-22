package tachyon.command;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.thrift.TException;

import tachyon.MasterClient;
import tachyon.thrift.DatasetInfo;

public class Ls {
  public static void main(String[] args) throws TException {
    if (args.length != 1) {
      System.out.println("Usage: tachyon\n [-ls <path>]");
      System.exit(-1);
    }

    System.out.println(args[0]);
    String path = args[0];
    if (!path.startsWith("tachyon://")) {
      System.out.println("The path name has to start with tachyon://");
      System.exit(1);
    }
    path = path.substring(10);
    if (!path.contains(":") || !path.contains("/")) {
      System.out.println("The path name is invalid.");
    }
    int pos = path.indexOf("/");
    String masterAddress = path.substring(0, pos);
    String folder = path.substring(pos);
    String masterHost = masterAddress.split(":")[0];
    int masterPort = Integer.parseInt(masterAddress.split(":")[1]);

    MasterClient masterClient = new MasterClient(new InetSocketAddress(masterHost, masterPort));
    masterClient.open();
    List<DatasetInfo> files = masterClient.cmd_ls(folder);
    System.out.println("The folder " + folder + " contains " + files.size() + " files");
    for (int i = 0; i < files.size(); i ++) {
      for (int j = i + 1; j < files.size(); j ++) {
        if (files.get(i).mPath.compareToIgnoreCase(files.get(j).mPath) > 0) {
          DatasetInfo tmp = files.get(i);
          files.set(i, files.get(j));
          files.set(j, tmp);
        }
      }
    }
    for (int k = 0; k < files.size(); k ++) {
      System.out.println(files.get(k));
    }
  }
}