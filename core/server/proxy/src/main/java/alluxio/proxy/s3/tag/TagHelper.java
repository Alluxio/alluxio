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

package alluxio.proxy.s3.tag;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.proxy.s3.S3RestServiceHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for handling tags according to https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
 * and https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html.
 */
public class TagHelper {
  private static final Logger LOG = LoggerFactory.getLogger(S3RestServiceHandler.class);

  private static final String TAG_FOLDER = "/_TAG";
  private static final String FOLDER_OBJECT_TAG_FILE = "_FOLDER_TAG";

  /**
   * @param fs the filesystem
   * @param path the filesystem path to update tags
   * @param is the input stream of XML containing new tags
   */
  public static void putTags(FileSystem fs, String path, InputStream is)
      throws IOException, AlluxioException {
    final String tagPath = getTagPath(fs, path);

    Map<String, String> tagMap = new HashMap<>();

    final AlluxioURI tagPathURI = new AlluxioURI(tagPath);
    LOG.info(tagPathURI.getPath());
    try {
      fs.delete(tagPathURI);
    } catch (FileDoesNotExistException e) {
      // ignore
    }

    final Tagging tagToUpdate = new XmlMapper().readValue(is, Tagging.class);

    final TagSet tagSet = tagToUpdate.getTagSet();
    if (tagSet != null) {
      for (Tag tag : tagSet.getTags()) {
        tagMap.put(tag.getKey(), tag.getValue());
      }
    }

    try (final FileOutStream os = fs.createFile(tagPathURI,
        CreateFilePOptions.newBuilder().setRecursive(true).build())) {
      new ObjectMapper().writeValue(os, tagMap);
    }
  }

  /**
   * @param fs the file system
   * @param path the path in the file system to look up tags
   * @return map of tag keys to tag values
   */
  public static Map<String, String> getTags(FileSystem fs, String path)
      throws IOException, AlluxioException {
    final String tagPath = getTagPath(fs, path);

    try {
      final FileInStream fis = fs.openFile(new AlluxioURI(tagPath));
      return new ObjectMapper().readValue(fis, Map.class);
    } catch (FileDoesNotExistException e) {
      return Collections.emptyMap();
    }
  }

  private static String getTagPath(FileSystem fs, String path)
      throws IOException, AlluxioException {
    final URIStatus status = fs.getStatus(new AlluxioURI(path));

    if (status.isFolder()) {
      return TAG_FOLDER + path + FOLDER_OBJECT_TAG_FILE;
    }
    return TAG_FOLDER + path;
  }
}
