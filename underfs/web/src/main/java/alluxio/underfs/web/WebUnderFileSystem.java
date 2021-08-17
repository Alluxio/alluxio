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

package alluxio.underfs.web;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.underfs.ConsistentUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.network.HttpUtils;

import com.google.common.io.ByteStreams;
import org.apache.http.Header;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Web {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class WebUnderFileSystem extends ConsistentUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(WebUnderFileSystem.class);
  private int mTimeout;
  private final String mUnsupportedMsg = "Unsupported operation for WebUnderFileSystem.";

  /**
   * Constructs a new {@link WebUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ufsConf UFS configuration
   */
  public WebUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    super(uri, ufsConf);
    mTimeout = (int) mUfsConf.getMs(PropertyKey.UNDERFS_WEB_CONNECTION_TIMEOUT);
  }

  @Override
  public String getUnderFSType() {
    return "web";
  }

  @Override
  public void cleanup() throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public void close() throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public boolean exists(String path) throws IOException {
    return HttpUtils.head(path, mTimeout) != null;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    if (!exists(path)) {
      throw new FileNotFoundException(path);
    }

    return mUfsConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    UfsStatus ufsStatus = getStatus(path);
    if (ufsStatus instanceof UfsDirectoryStatus) {
      return (UfsDirectoryStatus) ufsStatus;
    }

    throw new IOException("Failed to getDirectoryStatus: " + path);
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return new ArrayList<>();
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return getFileLocations(path);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    UfsStatus ufsStatus = getStatus(path);
    if (ufsStatus instanceof UfsFileStatus) {
      return (UfsFileStatus) ufsStatus;
    }
    throw new IOException("Failed to getFileStatus: " + path);
  }

  // This call is currently only used for the web ui, where a negative value implies unknown.
  @Override
  public long getSpace(String path, SpaceType type) {
    return -1;
  }

  /**
   * Get the file status of a http url.
   *
   * @param path the http url
   * @param fileName the file name
   * @return a UfsStatus object related to the http url
   */
  private UfsStatus getStatus(String path, String fileName) throws IOException {
    long contentLength = 0;
    long lastModified = new Date().getTime();
    Header[] headers = HttpUtils.head(path, mTimeout);
    if (headers == null) {
      throw new IOException("Failed to getStatus: " + path);
    }

    for (Header header : headers) {
      String headerName = header.getName();
      if (headerName.equalsIgnoreCase("Content-Length")) {
        contentLength = Long.parseLong(header.getValue());
      } else if (headerName.equalsIgnoreCase("Last-Modified")) {
        lastModified = parseTimestamp(header.getValue(),
            mUfsConf.get(PropertyKey.UNDERFS_WEB_HEADER_LAST_MODIFIED));
      }
    }

    if (isFile(path)) {
      // Return file status.
      String contentHash = UnderFileSystemUtils.approximateContentHash(contentLength, lastModified);
      return new UfsFileStatus(fileName == null ? path : fileName, contentHash, contentLength,
          lastModified, "", "", (short) 288,
          mUfsConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT));
    }
    // Return directory status.
    return new UfsDirectoryStatus(fileName == null ? path : fileName, "", "", (short) 800,
        lastModified);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    return getStatus(path, null);
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    String contentType = "";
    Header[] headers = HttpUtils.head(path, mTimeout);
    if (headers == null) {
      return false;
    }

    for (Header header : headers) {
      if (header.getName().equalsIgnoreCase("Content-Type")) {
        contentType = header.getValue();
        break;
      }
    }

    if (contentType.contains("text/html")) {
      Elements titleElements = Jsoup.connect(path).get().select("title");
      if (titleElements.size() > 0) {
        String title = titleElements.get(0).text();
        List<String> titles = mUfsConf.getList(PropertyKey.UNDERFS_WEB_TITLES, ",");
        for (final String t : titles) {
          if (title.contains(t)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return !isDirectory(path);
  }

  /**
   * Parse the string to a unix timestamp.
   *
   * @param datetime a date time string
   * @param format the pattern related to the datetime string
   * @return the unixstamp for the datetime string
   */
  private long parseTimestamp(String datetime, String format) {
    long timestamp = new Date().getTime();
    if (datetime.length() > 0 && datetime.compareTo("-") != 0) {
      try {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        Date parsedDate = dateFormat.parse(datetime);
        timestamp = parsedDate.getTime();
      } catch (Exception e) {
        LOG.error("Failed to format {} to a timestamp", datetime, e);
      }
    }
    return timestamp;
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    Document doc;
    try {
      doc = Jsoup.connect(path).get();
    } catch (Exception e) {
      LOG.error("Failed to get content from URL {}", path, e);
      return null;
    }

    Elements bodyElements = doc.select("body");
    if (bodyElements.size() == 0) {
      return null;
    }

    Element bodyElement = bodyElements.first();
    Elements bodyChildNodes = bodyElement.children();
    Elements listElements = null;
    for (Element bodyChildNode : bodyChildNodes) {
      listElements = bodyChildNode.select("a");
      if (listElements.size() > 0) {
        break;
      }
    }

    if (listElements == null || listElements.size() == 0) {
      return null;
    }

    List<String> parentNames = mUfsConf.getList(PropertyKey.UNDERFS_WEB_PARENT_NAMES, ",");
    int flagIndex = -1;
    for (int i = 0; flagIndex == -1 && i < listElements.size(); i++) {
      for (final String flag : parentNames) {
        if (listElements.get(i).text().equalsIgnoreCase(flag)) {
          flagIndex = i;
          break;
        }
      }
    }

    List<UfsStatus> statusList = new ArrayList<>();
    for (int i = (flagIndex == -1 ? 0 : flagIndex); i < listElements.size(); i++) {
      Element listElement = listElements.get(i);
      String href = listElement.attr("href");
      String fileName = listElement.text();
      if (!new WebUnderFileSystemFactory().supportsPath(href)) {
        href = path + "/" + href;
      }

      try {
        UfsStatus ufsStatus = getStatus(href, fileName);
        statusList.add(ufsStatus);
      } catch (IOException e) {
        LOG.error("Failed to get status for url: {}", href, e);
      }
    }

    UfsStatus[] rtn = new UfsStatus[statusList.size()];
    return statusList.toArray(rtn);
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    InputStream inputStream = HttpUtils.getInputStream(path, mTimeout);
    try {
      ByteStreams.skipFully(inputStream, options.getOffset());
    } catch (IOException e) {
      inputStream.close();
      throw e;
    }
    return inputStream;
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    throw new IOException(mUnsupportedMsg);
  }

  @Override
  public boolean supportsFlush() throws IOException {
    throw new IOException(mUnsupportedMsg);
  }
}
