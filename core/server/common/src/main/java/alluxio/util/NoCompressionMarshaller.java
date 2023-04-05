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

package alluxio.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Simple marshaller that applies no compression.
 */
public class NoCompressionMarshaller implements DirectoryMarshaller {
  @Override
  public long write(Path path, OutputStream outputStream) throws IOException, InterruptedException {
    long totalBytesCopied = 0;
    try (final Stream<Path> stream = Files.walk(path);
        DataOutputStream dataOS = new DataOutputStream(outputStream)) {
      List<Path> paths = stream.filter(subpath -> subpath.toFile().isFile())
          .collect(Collectors.toList());
      for (Path subpath : paths) {
        byte[] relativePath = path.relativize(subpath).toString().getBytes();
        dataOS.write(relativePath.length);
        outputStream.write(relativePath);
        try (InputStream fileIn = new BufferedInputStream(Files.newInputStream(subpath))) {
          dataOS.writeLong(FileUtils.sizeOf(subpath.toFile()));
          totalBytesCopied += IOUtils.copyLarge(fileIn, dataOS);
        }
      }
    }
    return totalBytesCopied;
  }

  @Override
  public long read(Path path, InputStream inputStream) throws IOException {
    long totalBytesRead = 0;
    try (DataInputStream dataIS = new DataInputStream(inputStream)) {
      int pathSize;
      while ((pathSize = dataIS.read()) != -1) {
        byte[] relativePath = new byte[pathSize];
        dataIS.read(relativePath);
        File filePath = new File(path.toFile(), new String(relativePath));
        filePath.getParentFile().mkdirs();
        long fileSize = dataIS.readLong();
        try (OutputStream fileOut =
                 new BufferedOutputStream(Files.newOutputStream(filePath.toPath()))) {
          totalBytesRead += IOUtils.copyLarge(dataIS, fileOut, 0, fileSize);
        }
      }
    }
    return totalBytesRead;
  }
}
