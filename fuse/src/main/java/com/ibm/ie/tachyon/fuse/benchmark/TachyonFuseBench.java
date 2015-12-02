/*
 * Licensed to IBM Ireland - Research and Development under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.ie.tachyon.fuse.benchmark;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.exception.TachyonException;

/**
 *
 * @author Andrea Reale <realean2@ie.ibm.com>
 */
public final class TachyonFuseBench {

  private final List<Integer> mWriteSizeMBytes;
  private final File mSrcFile;
  private final File mTachyonFusePath;
  private final File mResultsFile;
  private final TachyonURI mTachyonURI;
  private final TachyonFileSystem mTFS;
  private final int mBlockSize;
  private final int mRepetitions;


  public TachyonFuseBench(String resultsFile, String tachyonFusePath, String tachyonURI,
                          int blockSize, int repetitions,  String[] writeSizeBytes) {
    Preconditions.checkArgument(blockSize > 0);
    Preconditions.checkArgument(repetitions > 0);
    mSrcFile = new File("/dev/zero");
    mTachyonFusePath = new File(tachyonFusePath);
    mResultsFile = new File(resultsFile);
    mTFS = TachyonFileSystem.TachyonFileSystemFactory.get();
    mBlockSize = blockSize;
    mTachyonURI = new TachyonURI(tachyonURI);
    mRepetitions = repetitions;

    mWriteSizeMBytes = new ArrayList<>(writeSizeBytes.length);
    for (final String sz: writeSizeBytes) {
      mWriteSizeMBytes.add(Integer.parseInt(sz));
    }

  }


  private long benchWrite(final OutputStream os, long toWriteBytes)  throws IOException{
    long start = System.currentTimeMillis();
    BufferedInputStream is = null;
    try {
      is = new BufferedInputStream(new FileInputStream(mSrcFile));
      int nread = 0, nwritten = 0;
      byte[] buff = new byte[mBlockSize];
      while (nwritten < toWriteBytes) {
        nread = is.read(buff, 0, mBlockSize);
        if (nread > 0) {
          os.write(buff, 0, nread);
          nwritten += nread;
        }
      }

      System.err.println("Written " + nwritten + " bytes");
    } finally {
      if (is != null) {
          is.close();
      }
    }

    return System.currentTimeMillis() - start;
  }
  private long benchRead(final InputStream is)  throws IOException{
    long start = System.currentTimeMillis();

    int nread = 0, totalread =0;
    byte[] buff = new byte[mBlockSize];
    while (nread >= 0) {
      nread = is.read(buff, 0, mBlockSize);
      if (nread > 0) {
        totalread += nread;
      }
    }
    System.err.println("Read " + totalread + " bytes");

    return System.currentTimeMillis() - start;
  }


  private long[] benchFuse(long toWriteBytes) throws Exception {
    long writePath = 0, readPath = 0;
    FileOutputStream fos = null;
    FileInputStream fis = null;
    try {
      if (mTachyonFusePath.exists()) {
        mTachyonFusePath.delete();
      }
      fos = new FileOutputStream(mTachyonFusePath);
      writePath =  benchWrite(fos, toWriteBytes);
    } finally {
      if (fos != null) {
        fos.close();
      }
    }

    Thread.sleep(1000);

    try {
      fis = new FileInputStream(mTachyonFusePath);
      readPath = benchRead(fis);
    } finally {
      if (fis != null) {
        fis.close();
      }
    }

    return new long[] {writePath, readPath};

  }

  private long[] benchTachyon(long toWriteBytes) throws IOException, TachyonException {
    long writePath = 0, readPath = 0;
    OutputStream os = null;
    InputStream is = null;
    try {
      TachyonFile tf = mTFS.openIfExists(mTachyonURI);
      if (tf != null) { // file exists
        mTFS.delete(tf);
      }
      os = mTFS.getOutStream(mTachyonURI);
      writePath = benchWrite(os, toWriteBytes);
    } finally {
      if (os != null) {
        os.close();
      }
    }

    try {
      TachyonFile tf = mTFS.open(mTachyonURI);
      is = mTFS.getInStream(tf);
      readPath = benchRead(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }

    return new long[] {writePath, readPath};


  }

  private void benchMarkAll() throws Exception {
    PrintWriter pw = null;
    final boolean printHeader = !mResultsFile.exists();
    try {
      pw = new PrintWriter(new FileOutputStream(mResultsFile, true));
      if (printHeader) {
          pw.println("# type,r/w,size_mb,bs,repetition,time");
      }

      for (final long sz: mWriteSizeMBytes) {
        for (int rp = 1; rp <= mRepetitions; rp++) {
          final long szBytes = sz * 1024L * 1024L;

          System.out.println(rp + ": FUSE: " + sz + "MB");
          long[] fsTime = benchFuse(szBytes);
          pw.println(String.format("Tachyon-FUSE,w,%d,%d,%d,%d", sz, mBlockSize,
              rp, fsTime[0]));
          pw.println(String.format("Tachyon-FUSE,r,%d,%d,%d,%d", sz, mBlockSize,
              rp, fsTime[1]));

          System.out.println(rp + ": TACHYON: " + sz + "MB");
          long[] tcTime = benchTachyon(szBytes);
          pw.println(String.format("Tachyon,w,%d,%d,%d,%d", sz, mBlockSize,
              rp, tcTime[0]));
          pw.println(String.format("Tachyon,r,%d,%d,%d,%d", sz, mBlockSize,
              rp, tcTime[1]));
        }
      }
    } finally {
      if (pw != null) {
        pw.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 6 || "-h".equals(args[0])) {
      System.out.println(String.format("Usage: java %s <outCSV> <fusePath> <tachyonURI> " +
          "<blockSizeBytes> <repetitions> <writeSizeMB1> [<writeSizeMB2> ... ]", TachyonFuseBench
          .class.getName()));
      System.exit(0);
    }

    String outCsv = args[0];
    String fusePath = args[1];
    String tachyonURI = args[2];
    int blockSize = Integer.parseInt(args[3]);
    int repetitions = Integer.parseInt(args[4]);
    String [] rest = Arrays.copyOfRange(args,5,args.length);

    TachyonFuseBench b = new TachyonFuseBench(outCsv, fusePath, tachyonURI, blockSize,
        repetitions, rest);

    b.benchMarkAll();

  }





}
