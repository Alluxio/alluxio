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

package alluxio.s3;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.ArrayList;
import java.util.List;

/**
 * An object representing the response to a DeleteObjects request. See
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html for more information about
 * this type of request.
 */
@JacksonXmlRootElement(localName = "DeleteResult")
public class DeleteObjectsResult {

  @JacksonXmlProperty(localName = "Deleted")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<DeletedObject> mDeleted = new ArrayList<>();

  @JacksonXmlProperty(localName = "Error")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<ErrorObject> mErrored = new ArrayList<>();

  /**
   * @return the successfully deleted objects
   */
  @JacksonXmlProperty(localName = "Deleted")
  public List<DeletedObject> getDeleted() {
    return mDeleted;
  }

  /**
   * @return the objects that errored when deleting
   */
  @JacksonXmlProperty(localName = "Error")
  public List<ErrorObject> getErrored() {
    return mErrored;
  }

  /**
   * Sets the set of deleted objects.
   *
   * @param deleted the set to include in the response
   */
  @JacksonXmlProperty(localName = "Deleted")
  public void setDeleted(List<DeletedObject> deleted) {
    mDeleted = deleted;
  }

  /**
   * Sets the set of errored objects in the response.
   *
   * @param errored the set of errored objects
   */
  @JacksonXmlProperty(localName = "Error")
  public void setErrored(List<ErrorObject> errored) {
    mErrored = errored;
  }

  /**
   * An object representing Deleted result in DeleteObjects result.
   */
  @JacksonXmlRootElement(localName = "Deleted")
  public static class DeletedObject {

    @JacksonXmlProperty(localName = "Key")
    private String mKey;

    @JacksonXmlProperty(localName = "DeleteMarker")
    private String mDeleteMarker;

    @JacksonXmlProperty(localName = "DeleteMarkerVersionId")
    private String mDeleteMarkerVersionId;

    @JacksonXmlProperty(localName = "VersionId")
    private String mVersionId;

    /**
     * Constructs a {@link DeletedObject}.
     */
    public DeletedObject() { }

    /**
     * @return the successfully deleted object key
     */
    @JacksonXmlProperty(localName = "Key")
    public String getKey() {
      return mKey;
    }

    /**
     * Sets the key of successfully deleted object.
     *
     * @param key the key name of objects
     */
    @JacksonXmlProperty(localName = "Key")
    public void setKey(String key) {
      mKey = key;
    }

    /**
     * @return the DeleteMarker
     */
    @JacksonXmlProperty(localName = "DeleteMarker")
    public String getDeleteMarker() {
      return mDeleteMarker;
    }

    /**
     * Sets the delete marker of this delete result.
     *
     * @param marker the DeleteMarker
     */
    @JacksonXmlProperty(localName = "DeleteMarker")
    public void setDeleteMarker(String marker) {
      mDeleteMarker = marker;
    }

    /**
     * @return the DeleteMarkerVersionId
     */
    @JacksonXmlProperty(localName = "DeleteMarkerVersionId")
    public String getDeleteMarkerVersionId() {
      return mDeleteMarkerVersionId;
    }

    /**
     * Sets the version id of delete marker.
     *
     * @param deleteMarkerVersionId the version id of DeleteMarker
     */
    @JacksonXmlProperty(localName = "DeleteMarkerVersionId")
    public void setDeleteMarkerVersionId(String deleteMarkerVersionId) {
      mDeleteMarkerVersionId = deleteMarkerVersionId;
    }

    /**
     * @return the VersionId
     */
    @JacksonXmlProperty(localName = "VersionId")
    public String getVersionId() {
      return mVersionId;
    }

    /**
     * Sets the version id of deleted object.
     *
     * @param versionId the version id of object
     */
    @JacksonXmlProperty(localName = "VersionId")
    public void setVersionId(String versionId) {
      mVersionId = versionId;
    }
  }

  /**
   * An object representing error result in DeleteObjects result.
   */
  @JacksonXmlRootElement(localName = "Error")
  public static class ErrorObject {
    public String mKey;
    public String mCode;
    public String mMessage;
    public String mVersionId;

    /**
     * Constructs a {@link ErrorObject}.
     */
    public ErrorObject() {
      mKey = "";
      mCode = "";
      mMessage = "";
      mVersionId = "";
    }

    /**
     * @return the key of error object
     */
    @JacksonXmlProperty(localName = "Key")
    public String getKey() {
      return mKey;
    }

    /**
     * @return the code of error object
     */
    @JacksonXmlProperty(localName = "Code")
    public String getCode() {
      return mCode;
    }

    /**
     * @return the error message of error object
     */
    @JacksonXmlProperty(localName = "Message")
    public String getMessage() {
      return mMessage;
    }

    /**
     * @return the version id of error object
     */
    @JacksonXmlProperty(localName = "VersionId")
    public String getVersionId() {
      return mVersionId;
    }

    /**
     * Sets the key of error object.
     *
     * @param key the key of object
     */
    @JacksonXmlProperty(localName = "Key")
    public void setKey(String key) {
      mKey = key;
    }

    /**
     * Sets the code of this result.
     *
     * @param code the code
     */
    @JacksonXmlProperty(localName = "Code")
    public void setCode(String code) {
      mCode = code;
    }

    /**
     * Sets the error message of this result.
     *
     * @param message
     */
    @JacksonXmlProperty(localName = "Message")
    public void setMessage(String message) {
      mMessage = message;
    }

    /**
     * Sets the version id.
     *
     * @param versionId
     */
    @JacksonXmlProperty(localName = "VersionId")
    public void setVersionId(String versionId) {
      mVersionId = versionId;
    }
  }
}
