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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;

import java.io.IOException;

/**
 * A simple Alluxio App. Please Delete this from your final PR.
 *
 */
public class App {
  /**
   * Create a file and write data.
   * @param fs
   */
  public static void writeData(FileSystem fs) throws IOException, AlluxioException {
    // Create a file and get its output stream
    AlluxioURI pathw = new AlluxioURI("/MyFile001");
    FileOutStream out = fs.createFile(pathw);
    // Write data
    byte[] buf = new byte[4096];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) ('A' + i % 26);
    }
    out.write(buf);
    // Close and complete file
    out.close();
  }

  /**
   * The main function.
   *
   * @param args the command line args
   */
  public static void main(String[] args) throws
      alluxio.exception.AlluxioException,
      java.io.IOException
  {
    System.out.println("Hello World! This is a simple Alluxio App.");

    FileSystem fs = FileSystem.Factory.get();
    System.out.println(fs);

    writeData(fs);

    fs.close();
    System.out.println("Write File Done!");
    System.exit(0);
  }
}
