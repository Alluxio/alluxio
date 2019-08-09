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

package alluxio.master.catalog;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.util.io.PathUtils;
import com.google.common.base.Preconditions;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class AlluxioTableOperations implements TableOperations {
  enum TableSource
  {
    NATIVE,
    IMPORTED
  }
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioTableOperations.class);

  public static final String IMPORTED_DB_NAME = "Imported";
  private final String mDatabase;
  private final String mTableName;
  private final TableSource mSource;
  private FileIO mDefaultFileIo;
  private Integer mVersion;

  private TableMetadata mCurrentMetadata = null;
  private boolean mShouldRefresh = true;

  private final String mLocation;

  private final FileSystem mFileSystem;

  public AlluxioTableOperations(FileSystem fs, String location) {
    mSource = TableSource.IMPORTED;
    mLocation = location;
    mDatabase = IMPORTED_DB_NAME;
    mTableName = location;
    mFileSystem = fs;
  }

  public AlluxioTableOperations(FileSystem fs, String dbName, String tableName) {
    mSource = TableSource.NATIVE;
    mDatabase = dbName;
    mTableName = tableName;
    mLocation = PathUtils.concatPath(ServerConfiguration.get(PropertyKey.METADATA_PATH),
        dbName, tableName, "metadata");
    mFileSystem = fs;
  }

  public AlluxioTableOperations(FileSystem fs, String location, String dbName, String tableName) {
    mSource = TableSource.IMPORTED;
    mLocation = location;
    mDatabase = dbName;
    mTableName = tableName;
    mFileSystem = fs;
  }

  @Override
  public TableMetadata current() {
    if (mShouldRefresh) {
      return refresh();
    }
    return mCurrentMetadata;
  }

  private AlluxioURI versionHintFile() {
    return metadataPath("version-hint.text");
  }

  private AlluxioURI metadataPath(String filename) {
    return new AlluxioURI(PathUtils.concatPath(mLocation, "metadata", filename));
  }

  private AlluxioURI metadataFilePath(int metadataVersion, TableMetadataParser.Codec codec) {
    return metadataPath("v" + metadataVersion + TableMetadataParser.getFileExtension(codec));
  }

  private AlluxioURI getMetadataFile(int metadataVersion) throws IOException {
    for (TableMetadataParser.Codec codec : TableMetadataParser.Codec.values()) {
      AlluxioURI metadataFile = metadataFilePath(metadataVersion, codec);
      try {
        if (mFileSystem.exists(metadataFile)) {
          return metadataFile;
        }
      } catch (AlluxioException e) {
        return null;
      }
    }
    return null;
  }

  private int readVersionHint() {
    AlluxioURI versionHintFile = versionHintFile();
    try {
      if (!mFileSystem.exists(versionHintFile)) {
        return 0;
      }

      try (BufferedReader in = new BufferedReader(
          new InputStreamReader(mFileSystem.openFile(versionHintFile)))) {
        return Integer.parseInt(in.readLine().replace("\n", ""));
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", versionHintFile);
    } catch (AlluxioException e) {
      return 0;
    }
  }

  @Override
  public TableMetadata refresh() {
    int ver = mVersion != null ? mVersion : readVersionHint();
    try {
      AlluxioURI metadataFile = getMetadataFile(ver);
      if (mVersion == null && metadataFile == null && ver == 0) {
        // no v0 metadata means the table doesn't exist yet
        return null;
      } else if (metadataFile == null) {
        throw new ValidationException("Metadata file for version %d is missing", ver);
      }

      AlluxioURI nextMetadataFile = getMetadataFile(ver + 1);
      while (nextMetadataFile != null) {
        ver += 1;
        metadataFile = nextMetadataFile;
        nextMetadataFile = getMetadataFile(ver + 1);
      }

      mVersion = ver;

      TableMetadata newMetadata = TableMetadataParser.read(this, io().newInputFile(metadataFile.toString()));
      String newUUID = newMetadata.uuid();
      if (mCurrentMetadata != null) {
        Preconditions.checkState(newUUID == null || newUUID.equals(mCurrentMetadata.uuid()),
            "Table UUID does not match: current=%s != refreshed=%s", mCurrentMetadata.uuid(), newUUID);
      }

      mCurrentMetadata = newMetadata;
      mShouldRefresh = false;
      return mCurrentMetadata;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to refresh the table");
    }
  }

  @Override
  public void commit(TableMetadata oldMetadata, TableMetadata newMetadata) {

  }

  @Override
  public FileIO io() {
    if (mDefaultFileIo == null) {
      mDefaultFileIo = new AlluxioFileIO(mFileSystem);
    }
    return mDefaultFileIo;
  }
  @Override
  public String metadataFileLocation(String fileName) {
    return metadataPath(fileName).toString();
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }
}
