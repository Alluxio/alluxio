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

package tachyon.master.next.filesystem.journal.json;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectWriter;

import tachyon.master.next.filesystem.journal.ImageReadWriter;
import tachyon.master.next.filesystem.journal.InodeDirectoryImage;
import tachyon.master.next.filesystem.journal.InodeFileImage;
import tachyon.master.next.filesystem.journal.InodeImage;
import tachyon.master.next.journal.ImageType;

public class JsonImageReadWriter extends ImageReadWriter {
  private ObjectWriter mWriter = JsonObject.createObjectMapper().writer();

  @Override
  protected void writeInodeFileImage(InodeFileImage image, OutputStream os) throws IOException {
    JsonImage jsonImage =
        new JsonImage(ImageType.INODE_FILE).withParameter("creationTimeMs", image.creationTimeMs())
            .withParameter("id", image.id()).withParameter("name", image.name())
            .withParameter("parentId", image.parentId())
            .withParameter("blockSizeBytes", image.blockSizeBytes())
            .withParameter("length", image.length()).withParameter("complete", image.isComplete())
            .withParameter("pin", image.isPinned()).withParameter("cache", image.isCache())
            .withParameter("ufsPath", image.ufsPath())
            .withParameter("lastModificationTimeMs", image.lastModificationTimeMs());

    writeJsonImage(jsonImage, os);
  }

  @Override
  protected void writeInodeDirectoryImage(InodeDirectoryImage image, OutputStream os)
      throws IOException {
    JsonImage jsonImage =
        new JsonImage(ImageType.INODE_DIRECTORY)
            .withParameter("creationTimeMs", image.creationTimeMs())
            .withParameter("id", image.id()).withParameter("name", image.name())
            .withParameter("parentId", image.parentId()).withParameter("pinned", image.isPinned())
            .withParameter("childrenIds", image.childrenIds())
            .withParameter("lastModificationTimeMs", image.lastModificationTimeMs());

    writeJsonImage(jsonImage, os);

    for (InodeImage img : image.childrenImages()) {
      if (img.type() == ImageType.INODE_DIRECTORY) {
        writeInodeDirectoryImage((InodeDirectoryImage) img, os);
      } else {
        writeInodeFileImage((InodeFileImage) img, os);
      }
    }
  }

  private void writeJsonImage(JsonImage image, OutputStream os) throws IOException {
    mWriter.writeValue(os, image);
    (new DataOutputStream(os)).writeByte('\n');
  }
}
