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
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Create a new Alluxio file system, generating empty files in the root directory with random name.
 */
public class LoadGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static class RandomString {

    private static final char[] SYMBOLS;

    private final Random mRandom = new Random();

    private final char[] mBuf;

    static {
      StringBuilder tmp = new StringBuilder();
      for (char ch = '0'; ch <= '9'; ++ch) {
        tmp.append(ch);
      }
      for (char ch = 'a'; ch <= 'z'; ++ch) {
        tmp.append(ch);
      }
      SYMBOLS = tmp.toString().toCharArray();
    }

    RandomString(int length) {
      if (length < 1) {
        throw new IllegalArgumentException("length < 1: " + length);
      }
      mBuf = new char[length];
    }

    String nextString() {
      mBuf[0] = '/';
      for (int idx = 1; idx < mBuf.length; ++idx) {
        mBuf[idx] = SYMBOLS[mRandom.nextInt(SYMBOLS.length)];
      }
      return new String(mBuf);
    }
  }

  /**
   * Constructor.
   */
  public LoadGenerator() {}

  /**
   * Create files in flat hierarchy and pause according to file number interval.
   *
   * @param args could provide one parameter of file number
   */
  public static void main(String[] args) {
    AlluxioURI file;
    FileOutStream out;
    CreateFileOptions options = CreateFileOptions.defaults();
    int filenumber = 8000000;
    if (args.length == 1) {

      filenumber = Integer.parseInt(args[0]);
    }

    System.out.printf("start, default file number %d%n", filenumber);
    FileSystem fs = FileSystem.Factory.get();
    RandomString fileNameGenerator = new RandomString(6);
    try {
      for (int i = 0; i < filenumber; i++) {
        file = new AlluxioURI(fileNameGenerator.nextString() + "A" + Integer.toString(i));

        out = fs.createFile(file, options);
        out.close();

        if (i % 500000 == 0) {
          System.out.printf("File Number: %d, Press enter to continue%n", i);
        }
      }
    } catch (IOException | AlluxioException e) {
      e.printStackTrace();
    }
  }
}

