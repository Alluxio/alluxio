
package tachyon.command;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.StringUtils;

import org.apache.thrift.TException;
import tachyon.MasterClient;
import tachyon.thrift.DatasetInfo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class TFsShell extends Configured implements Tool {

	public TFsShell() {}

	//public TFsShell(Configuration conf) {}

	public void printUsage(String cmd) {}
	public void close() {}
	//public void init() {}

	private int ls(String path, boolean recursive) throws TException{
	    String folder = Utils.getDatasetName(path);
	    MasterClient masterClient = new MasterClient(Utils.getTachyonMasterAddress(path));
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
	      System.out.println(files.get(k) + " file(s) in total.");
	    }

		return 0;
	}

	public int rm(String path, boolean recursive) throws TException{
	    String folder = Utils.getDatasetName(path);
	    MasterClient masterClient = new MasterClient(Utils.getTachyonMasterAddress(path));
	    masterClient.open();
	    List<DatasetInfo> files = masterClient.cmd_rm(folder);
	    System.out.println("The folder " + folder + " has been removed: ");
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
	      System.out.println(files.get(k) + " file(s) in total.");
	    }
		return 0;
	}

	public int mkdir(String path) {
		String folder = Utils.getDatasetName(path);
		MasterClient masterClient = new MasterClient(Utils.getTachyonMasterAddress(path));
		masterClient.open();
		masterClient.cmd_mkdir(folder);
		System.out.println(folder + " has been created: ");
		return 0;
	}

	/*public int createDataset(String datasetPath, int partitions, String hdfsPath) {
		// verify partitions size not too excessive
		// verify paths?
		user_createDataset(datasetPath, partitions, hdfsPath);
		return 0;
	}*/

	private int doall(String cmd, String argv[], int startindex) {
		int exitCode = 0;
		for (int i = startindex; i < argv.length; i++) {
			try {
				if ("-cat".equals(cmd)) {

				} 
				else if ("-mkdir".equals(cmd)) {
					exitCode = mkdir(argv[i]);
				}
				else if ("-ls".equals(cmd)) {
					exitCode = ls(argv[i], false);
				}
				else if ("-lsr".equals(cmd)) {
					exitCode = ls(argv[i], true);
				}
				else if ("-rm".equals(cmd)) {
					exitCode = rm(argv[i], false);
				}
				else if ("-rmr".equals(cmd)) {
					exitCode = rm(argv[i], true);
				}
			} catch (Exception e) {
				e.printStackTrace();
				exitCode = 1;
				// handle accordingly
			}
		}
		return exitCode;
	}


	public int run(String argv[]) throws Exception {
	    if (argv.length < 1) {
	      printUsage(""); 
	      return -1;
	    }

	    int exitCode = -1;
	    int i = 0;
	    String cmd = argv[i++];

	    //
	    // verify that we have enough command line parameters
	    //
	    if ("-put".equals(cmd) || "-test".equals(cmd) ||
	        "-copyFromLocal".equals(cmd) || "-moveFromLocal".equals(cmd)) {
	      if (argv.length < 3) {
	        printUsage(cmd);
	        return exitCode;
	      }
	    } else if ("-get".equals(cmd) || 
	               "-copyToLocal".equals(cmd) || "-moveToLocal".equals(cmd)) {
	      if (argv.length < 3) {
	        printUsage(cmd);
	        return exitCode;
	      }
	    } else if ("-mv".equals(cmd) || "-cp".equals(cmd)) {
	      if (argv.length < 3) {
	        printUsage(cmd);
	        return exitCode;
	      }
	    } else if ("-rm".equals(cmd) || "-rmr".equals(cmd) ||
	               "-cat".equals(cmd) || "-mkdir".equals(cmd) ||
	               "-touchz".equals(cmd) || "-stat".equals(cmd) ||
	               "-text".equals(cmd)) {
	      if (argv.length < 2) {
	        printUsage(cmd);
	        return exitCode;
	      }
	    }

	    // initialize FsShell
	    /*try {
	      init();
	    } catch (Exception e) {	      
	      return exitCode;
	    }*/

	    exitCode = 0;
	    try {
	      if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd)) {
	        
	      } else if ("-moveFromLocal".equals(cmd)) {
	        
	      } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd)) {
	        
	      } else if ("-moveToLocal".equals(cmd)) {
	        
	      } else if ("-ls".equals(cmd)) {
	      	if (i < argv.length) {
	      		exitCode = doall(cmd, argv, i);
	      	} 
	      } else if ("-lsr".equals(cmd)) {
	        if (i < argv.length) {
	      		exitCode = doall(cmd, argv, i);
	      	} 
	      } else if ("-mv".equals(cmd)) {
	        
	      } else if ("-cp".equals(cmd)) {
	        
	      } else if ("-rm".equals(cmd)) {
	      	if (i < argv.length) {
	      		exitCode = doall(cmd, argv, i);
	      	} 
	      } else if ("-rmr".equals(cmd)) {
	      	if (i < argv.length) {
	      		exitCode = doall(cmd, argv, i);
	      	} 
	      } else if ("-expunge".equals(cmd)) {
	       
	      } else if ("-du".equals(cmd)) {
	       
	      } else if ("-dus".equals(cmd)) {
	       
	      } else if ("-mkdir".equals(cmd)) {
	       	if (i < argv.length) {
	      		exitCode = doall(cmd, argv, i);
	      	} 
	      } else if ("-touchz".equals(cmd)) {
	       
	      } else if ("-test".equals(cmd)) {
	       
	      } else if ("-stat".equals(cmd)) {
	       
	      } else if ("-help".equals(cmd)) {
	       
	      } else if ("-tail".equals(cmd)) {
	       
	      } else {
	       
	      }
	    } catch (IllegalArgumentException arge) {
	      exitCode = -1;
	      printUsage(cmd);
	    } catch (Exception e) {
	      
	    }

    	return exitCode;
  	}

	public static void main (String[] args) throws Exception {
		TFsShell shell = new TFsShell();
		int res;
		try {
			res = ToolRunner.run(shell, args);
		}
		finally {
			shell.close();
		}
		System.exit(res);
	}
}