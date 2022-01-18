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

package alluxio.underfs.ozone;

import alluxio.underfs.hdfs.HdfsUnderFileOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Output stream implementation for {@link OzoneUnderFileOutputStream}.
 */
@NotThreadSafe
public class OzoneUnderFileOutputStream extends HdfsUnderFileOutputStream {
  /**
   * Basic constructor.
   *
   * @param out underlying stream to wrap
   */
  public OzoneUnderFileOutputStream(FSDataOutputStream out) {
    super(out);
  }

  @Override
  public void close() throws IOException {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      super.close();
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void flush() throws IOException {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      super.flush();
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void write(int b) throws IOException {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      super.write(b);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      super.write(b);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      super.write(b, off, len);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }
}
