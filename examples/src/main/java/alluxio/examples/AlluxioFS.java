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

package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;

/**
 * Example program to use Alluxio native API to read and write files.
 *
 */
public class AlluxioFS
{
  /**
   * Entry point for the {@link AlluxioFS} program.
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) throws
      alluxio.exception.FileAlreadyExistsException,
      alluxio.exception.InvalidPathException,
      alluxio.exception.AlluxioException,
      java.io.IOException
  {
    final int bufSize = alluxio.Constants.KB * 4;
    System.out.println("Start Alluxio Native FS write/read");
    FileSystem fs = FileSystem.Factory.get();
    AlluxioURI path = new AlluxioURI("/AlluxioFS.txt");

    FileOutStream out = fs.createFile(path);
    byte[] buffout = new byte[bufSize];
    for (int i = 0; i < bufSize; i++) {
      buffout[i] = (byte) ('A' + i % 26);
    }
    out.write(buffout);
    out.close();
    System.out.println("End write");

    FileInStream in = fs.openFile(path);
    byte[] buffin = new byte[bufSize];
    in.read(buffin);
    in.close();
    System.out.println("End read:" + new String(buffin));
  }
}
