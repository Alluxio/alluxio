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

package alluxio.proxy.s3;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Parsed version of a request to DeleteObjects.
 * See https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
 */
@JacksonXmlRootElement(localName = "Delete")
public class DeleteObjectsRequest {

  @JacksonXmlProperty(localName = "Quiet")
  private boolean mQuiet;

  @JacksonXmlProperty(localName = "Object")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<DeleteObject> mToDelete;

  /**
   * Default constructor for jackson.
   */
  public DeleteObjectsRequest() {
    mQuiet = true;
    mToDelete = new ArrayList<>();
  }

  /**
   * Create a new {@link DeleteObjectsRequest}.
   *
   * @param quiet whether or not quiet response is enabled
   * @param objects the objects to delete
   */
  public DeleteObjectsRequest(boolean quiet, List<DeleteObject> objects) {
    mQuiet = quiet;
    mToDelete = objects;
  }

  /**
   * sets the quiet parameter.
   *
   * @param quiet whether to use a quiet response
   */
  @JacksonXmlProperty(localName = "Quiet")
  public void setQuiet(boolean quiet) {
    mQuiet = quiet;
  }

  /**
   * Set of items to delete.
   *
   * @param toDelete set of objects to delete
   */
  @JacksonXmlProperty(localName = "Object")
  public void setDeleteObject(List<DeleteObject> toDelete) {
    mToDelete = toDelete;
  }

  /**
   * @return whether this is a quiet request
   */
  @JacksonXmlProperty(localName = "Quiet")
  public boolean getQuiet() {
    return mQuiet;
  }

  /**
   * @return the objects to delete
   */
  @JacksonXmlProperty(localName = "Object")
  public List<DeleteObject> getToDelete() {
    return mToDelete;
  }

  /**
   * Inner POJO representing an object to delete in the S3 API.
   */
  @JacksonXmlRootElement(localName = "Object")
  static class DeleteObject {
    private String mKey;

    /**
     * Default constructor for jackson.
     */
    public DeleteObject() { }

    /**
     * @param key the key indicating the name of the object
     */
    public DeleteObject(String key) {
      mKey = key;
    }

    /**
     * @return the key of the object
     */
    @JacksonXmlProperty(localName = "Key")
    public String getKey() {
      return mKey;
    }

    /**
     * Sets the key to the object.
     *
     * @param key the key
     */
    @JacksonXmlProperty(localName = "Key")
    public void setKey(String key) {
      mKey = key;
    }
  }
}
