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

  @JacksonXmlRootElement(localName = "Deleted")
  static class DeletedObject {

    @JacksonXmlProperty(localName = "Key")
    private String mKey;

    @JacksonXmlProperty(localName = "DeleteMarker")
    private String mDeleteMarker;

    @JacksonXmlProperty(localName = "DeleteMarkerVersionId")
    private String mDeleteMarkerVersionId;

    @JacksonXmlProperty(localName = "VersionId")
    private String mVersionId;

    public DeletedObject() { }

    public DeletedObject(String key) {
      mKey = key;
    }

    @JacksonXmlProperty(localName = "Key")
    public String getKey() {
      return mKey;
    }

    @JacksonXmlProperty(localName = "Key")
    public void setKey(String key) {
      mKey = key;
    }

    @JacksonXmlProperty(localName = "DeleteMarker")
    public String getDeleteMarker() {
      return mDeleteMarker;
    }

    @JacksonXmlProperty(localName = "DeleteMarker")
    public void setDeleteMarker(String marker) {
      mDeleteMarker = marker;
    }

    @JacksonXmlProperty(localName = "DeleteMarkerVersionId")
    public String getDeleteMarkerVersionId() {
      return mDeleteMarkerVersionId;
    }

    @JacksonXmlProperty(localName = "DeleteMarkerVersionId")
    public void setDeleteMarkerVersionId(String deleteMarkerVersionId) {
      mDeleteMarkerVersionId = deleteMarkerVersionId;
    }

    @JacksonXmlProperty(localName = "VersionId")
    public String getVersionId() {
      return mVersionId;
    }

    @JacksonXmlProperty(localName = "VersionId")
    public void setVersionId(String versionId) {
      mVersionId = versionId;
    }
  }

  @JacksonXmlRootElement(localName = "Error")
  static class ErrorObject {
    public String mKey;
    public String mCode;
    public String mMessage;
    public String mVersionId;

    public ErrorObject() {
      mKey = "";
      mCode = "";
      mMessage = "";
      mVersionId = "";
    }

    @JacksonXmlProperty(localName = "Key")
    public String getKey() {
      return mKey;
    }

    @JacksonXmlProperty(localName = "Code")
    public String getCode() {
      return mCode;
    }

    @JacksonXmlProperty(localName = "Message")
    public String getMessage() {
      return mMessage;
    }

    @JacksonXmlProperty(localName = "VersionId")
    public String getVersionId() {
      return mVersionId;
    }

    @JacksonXmlProperty(localName = "Key")
    public void setKey(String key) {
      mKey = key;
    }

    @JacksonXmlProperty(localName = "Code")
    public void setCode(String code) {
      mCode = code;
    }

    @JacksonXmlProperty(localName = "Message")
    public void setMessage(String message) {
      mMessage = message;
    }

    @JacksonXmlProperty(localName = "VersionId")
    public void setVersionId(String versionId) {
      mVersionId = versionId;
    }
  }
}
