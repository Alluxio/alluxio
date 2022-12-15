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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Example program to use HDFS API to read and write files.
 *
 */
public class HDFS
{
  /**
   * Entry point for the {@link HDFS} program.
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) throws
      java.io.IOException
  {
    System.out.println("Starting write");

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://localhost:9000");
    conf.set("dfs.replication", "1");

    FileSystem fs = FileSystem.get(conf);

    String filename = "/HDFS.txt";

    FSDataOutputStream out = fs.create(new Path(filename));
    byte[] buffout = new byte[4096];
    for (int i = 0; i < 4096; i++) {
      buffout[i] = (byte) ('a' + i % 26);
    }
    out.write(buffout);
    out.close();

    FSDataInputStream in = fs.open(new Path(filename));
    byte[] buffin = new byte[4096];
    in.read(buffin);
    in.close();
    System.out.println("Got: " + (new String(buffin)));
    System.out.println("End write");
  }
}
