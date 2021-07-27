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

import alluxio.client.file.URIStatus;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of ListAllMyBucketsResult according to https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html.
 */
@JacksonXmlRootElement(localName = "ListAllMyBucketsResult")
public class ListAllMyBucketsResult {
  private List<Bucket> mBuckets;

  /**
   * Creates a {@link ListAllMyBucketsResult}.
   *
   * @param names names of all of the buckets
   */
  public ListAllMyBucketsResult(List<URIStatus> names) {
    mBuckets =
        names.stream().map((uriStatus) -> new Bucket(uriStatus.getName(),
            LocalDateTime.ofEpochSecond(
                uriStatus.getCreationTimeMs() / 1000L, 0, ZoneOffset.UTC).toString()))
            .collect(Collectors.toList());
  }

  /**
   * @return the list of buckets
   */
  @JacksonXmlProperty(localName = "Bucket")
  @JacksonXmlElementWrapper(localName = "Buckets")
  public List<Bucket> getBuckets() {
    return mBuckets;
  }

  /**
   * The Bucket object.
   */
  @JacksonXmlRootElement(localName = "Bucket")
  public class Bucket {
    private String mName;
    private String mCreationDate;

    private Bucket(String name, String creationDate) {
      mName = name;
      mCreationDate = creationDate;
    }

    /**
     * @return the name of the bucket
     */
    @JacksonXmlProperty(localName = "Name")
    public String getName() {
      return mName;
    }

    /**
     * @return the creation timestamp for the bucket
     */
    @JacksonXmlProperty(localName = "CreationDate")
    public String getCreationDate() {
      return mCreationDate;
    }
  }
}
