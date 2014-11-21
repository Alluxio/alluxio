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

package tachyon.master;

import java.io.DataOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Throwables;

/**
 * Class implemented this interface will be able to write image file.
 */
public abstract class ImageWriter {
  /**
   * Write image to the specified DataOutputStream. Use the specified ObjectWriter.
   * 
   * @param objWriter The used object writer
   * @param dos The target data output stream
   * @throws IOException
   */
  abstract void writeImage(ObjectWriter objWriter, DataOutputStream dos) throws IOException;

  /**
   * Write an ImageElement to the specified DataOutputStream. Use the specified ObjectWriter.
   * 
   * @param objWriter The used object writer
   * @param dos The target data output stream
   * @param ele The image element to be written
   */
  protected void writeElement(ObjectWriter objWriter, DataOutputStream dos, ImageElement ele) {
    try {
      objWriter.writeValue(dos, ele);
      dos.writeByte('\n');
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
