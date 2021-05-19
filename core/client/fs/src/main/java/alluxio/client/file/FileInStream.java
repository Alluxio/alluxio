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

import alluxio.Seekable;
import alluxio.client.BoundedStream;
import alluxio.client.PositionedReadable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
 */
public abstract class FileInStream extends InputStream implements BoundedStream, PositionedReadable,
    Seekable {
    public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
        int nread = 0;
        int rd = 0;
        final int sz = len;
        final byte[] dest = new byte[sz];
        while (rd >= 0 && nread < sz) {
            rd = read(dest, nread, sz - nread);
            if (rd >= 0) {
                nread += rd;
            }
        }
        if (nread == -1) { // EOF
            nread = 0;
        } else if (nread > 0) {
            byteBuffer.put(dest, 0, nread);
        }
        return nread;
    }
}
