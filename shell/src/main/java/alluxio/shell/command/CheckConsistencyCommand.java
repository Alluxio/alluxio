/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CheckConsistencyOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.wire.LoadMetadataType;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Command for checking the consistency of a file or folder between Alluxio and the under storage.
 */
public class CheckConsistencyCommand extends AbstractShellCommand {
  /**
   * @param fs the filesystem of Alluxio
   */
  public CheckConsistencyCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  protected Options getOptions() {
    return new Options().addOption(FIX_INCONSISTENT_FILES);
  }

  @Override
  public String getCommandName() {
    return "checkConsistency";
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI root = new AlluxioURI(args[0]);
    checkConsistency(root, cl.hasOption("r"));
  }

  private void checkConsistency(AlluxioURI path, boolean repairConsistency) throws
      AlluxioException, IOException {
    //load the Newly created file's metadata in alluxio
    ListStatusOptions listOptions = ListStatusOptions.defaults();
    listOptions.setLoadMetadataType(LoadMetadataType.Always);
    mFileSystem.listStatus(path, listOptions);
    CheckConsistencyOptions options = CheckConsistencyOptions.defaults();
    List<AlluxioURI> inconsistentUris = FileSystemUtils.checkConsistency(path, options);
    if (!repairConsistency) {
      if (inconsistentUris.isEmpty()) {
        System.out.println(path + " is consistent with the under storage system.");
      } else {
        Collections.sort(inconsistentUris);
        System.out.println("The following files are inconsistent:");
        for (AlluxioURI uri : inconsistentUris) {
          System.out.println(uri);
        }
      }
    } else {
      if (inconsistentUris.isEmpty()) {
        System.out.println(path + " is consistent with the under storage system.");
      } else {
        Collections.sort(inconsistentUris);
        System.out.println(path + " have: " + inconsistentUris.size() + " files inconsistent.");
        List<AlluxioURI> dirInconsistencys = new ArrayList<AlluxioURI>();
        for (int i = 0; i < inconsistentUris.size(); i++) {
          URIStatus status = mFileSystem.getStatus(inconsistentUris.get(i));
          if (status.isFolder()) {
            dirInconsistencys.add(inconsistentUris.get(i));
            continue;
          }
          System.out.println("repairing path: " + inconsistentUris.get(i));
          DeleteOptions deleteOptions = DeleteOptions.defaults().setAlluxioOnly(true);
          mFileSystem.delete(inconsistentUris.get(i), deleteOptions);
          Closer closer = Closer.create();
          try {
            OpenFileOptions openFileOptions = OpenFileOptions.defaults()
                .setReadType(ReadType.CACHE_PROMOTE);
            if (mFileSystem.exists(inconsistentUris.get(i))) {
              if (status.getInMemoryPercentage() == 100) {
                FileInStream in = closer.register(mFileSystem.openFile(inconsistentUris.get(i),
                    openFileOptions));
                byte[] buf = new byte[8 * Constants.MB];
                while (in.read(buf) != -1) {
                }
              }
            }
          } catch (Exception e) {
            throw closer.rethrow(e);
          } finally {
            closer.close();
          }
          System.out.println(inconsistentUris.get(i) + " repaired");
          System.out.println();
        }
        for (AlluxioURI uri : dirInconsistencys) {
          DeleteOptions deleteOptions = DeleteOptions.defaults().setAlluxioOnly(true)
              .setRecursive(true);
          System.out.println("repairing path: " + uri);
          mFileSystem.delete(uri, deleteOptions);
          System.out.println(uri + "repaired");
          System.out.println();
        }
      }
    }
  }

  @Override
  public String getUsage() {
    return "checkConsistency [-r] <Alluxio path>";
  }

  @Override
  public String getDescription() {
    return "Checks the consistency of a persisted file or directory in Alluxio. Any files or "
        + "directories which only exist in Alluxio or do not match the metadata of files in the "
        + "under storage will be returned. An administrator should then reconcile the differences."
        + "Specify -r to repair the inconsistent files.";
  }
}
