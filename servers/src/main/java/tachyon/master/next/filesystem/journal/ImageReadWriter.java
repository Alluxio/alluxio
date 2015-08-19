/*
 * Licensed to the University of California, Berkeley under one or more contributor license
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

package tachyon.master.next.filesystem.journal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import tachyon.master.next.journal.Image;

public abstract class ImageReadWriter implements tachyon.master.next.journal.ImageReadWriter {
  @Override
  public void writeImage(Image image, OutputStream os) throws IOException {
    switch (image.type()) {
      case INODE_FILE: {
        writeInodeFileImage((InodeFileImage) image, os);
        break;
      }
      case INODE_DIRECTORY: {
        writeInodeDirectoryImage((InodeDirectoryImage) image, os);
        break;
      }
      default:
        throw new IOException("Invalid image type " + image.type());
    }
  }

  @Override
  public Image readImage(InputStream is) throws IOException {
    // TODO(cc)
    return null;
  }

  protected abstract void writeInodeFileImage(InodeFileImage image, OutputStream os)
      throws IOException;

  protected abstract void writeInodeDirectoryImage(InodeDirectoryImage image, OutputStream os)
      throws IOException;
}
