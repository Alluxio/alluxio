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

import alluxio.grpc.FileInfo;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPartialPResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains information about the result of a partial listing.
 */
public class ListStatusPartialResult {

  /**
   * Generate a {@link ListStatusPartialResult} from a {@link ListStatusPartialPResponse}.
   *
   * @param response the original response
   * @return the converted response
   */
  public static ListStatusPartialResult fromProto(ListStatusPartialPResponse response) {
    ArrayList<URIStatus> listings = new ArrayList<>(response.getFileInfosCount());
    for (FileInfo info : response.getFileInfosList()) {
      listings.add(new URIStatus(GrpcUtils.fromProto(info)));
    }
    return new ListStatusPartialResult(listings, response.getIsTruncated(),
        response.getFileCount());
  }

  private final List<URIStatus> mListings;
  private final boolean mTruncated;
  private final long mFileCount;

  private ListStatusPartialResult(List<URIStatus> listings, boolean isTruncated, long fileCount) {
    mListings = listings;
    mTruncated = isTruncated;
    mFileCount = fileCount;
  }

  /**
   * @return the listings
   */
  public List<URIStatus> getListings() {
    return mListings;
  }

  /**
   * @return true if the listing was truncated
   */
  public boolean isTruncated() {
    return mTruncated;
  }

  /**
   * @return the total number of files in the listed directory,
   *  (i.e. the size of the result if partial listing was not used
   *  or -1 if the listing was recursive)
   */
  public long getFileCount() {
    return mFileCount;
  }
}
