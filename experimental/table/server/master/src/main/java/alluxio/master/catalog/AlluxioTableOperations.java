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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

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

  private final UnderFileSystem mFileSystem;

  public AlluxioTableOperations(UnderFileSystem fs, String location) {
    mSource = TableSource.IMPORTED;
    mLocation = location;
    mDatabase = IMPORTED_DB_NAME;
    mTableName = location;
    mFileSystem = fs;
  }

  public AlluxioTableOperations(UnderFileSystem fs, String dbName, String tableName) {
    mSource = TableSource.NATIVE;
    mDatabase = dbName;
    mTableName = tableName;
    mLocation = PathUtils.concatPath(ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS),
        ServerConfiguration.get(PropertyKey.METADATA_PATH),
        dbName, tableName);
    mFileSystem = fs;
  }

  public AlluxioTableOperations(UnderFileSystem fs, String location, String dbName, String tableName) {
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

  private String versionHintFile() {
    return metadataPath("version-hint.text");
  }

  private String metadataPath(String filename) {
    return PathUtils.concatPath(mLocation, "metadata", filename);
  }

  private String metadataFilePath(int metadataVersion, TableMetadataParser.Codec codec) {
    return metadataPath("v" + metadataVersion + TableMetadataParser.getFileExtension(codec));
  }

  private String getMetadataFile(int metadataVersion) throws IOException {
    for (TableMetadataParser.Codec codec : TableMetadataParser.Codec.values()) {
      String metadataFile = metadataFilePath(metadataVersion, codec);
      if (mFileSystem.exists(metadataFile)) {
        return metadataFile;
      }
    }
    return null;
  }

  private int readVersionHint() {
    String versionHintFile = versionHintFile();
    try {
      if (!mFileSystem.exists(versionHintFile)) {
        return 0;
      }

      try (BufferedReader in = new BufferedReader(
          new InputStreamReader(mFileSystem.open(versionHintFile)))) {
        return Integer.parseInt(in.readLine().replace("\n", ""));
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", versionHintFile);
    }
  }

  @Override
  public TableMetadata refresh() {
    int ver = mVersion != null ? mVersion : readVersionHint();
    try {
      String metadataFile = getMetadataFile(ver);
      if (mVersion == null && metadataFile == null && ver == 0) {
        // no v0 metadata means the table doesn't exist yet
        return null;
      } else if (metadataFile == null) {
        throw new ValidationException("Metadata file for version %d is missing", ver);
      }

      String nextMetadataFile = getMetadataFile(ver + 1);
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

  private String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
    String codecName = meta.property(
        TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    String fileExtension = TableMetadataParser.getFileExtension(codecName);
    return metadataFileLocation(String.format("%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
  }

  protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
    String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
    OutputFile newMetadataLocation = io().newOutputFile(newTableMetadataFilePath);

    // write the new metadata
    TableMetadataParser.write(metadata, newMetadataLocation);

    return newTableMetadataFilePath;
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
      throw new CommitFailedException("Cannot commit: stale table metadata for %s.%s", mDatabase, mTableName);
    }

    // if the metadata is not changed, return early
    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }String codecName = metadata.property(
        TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromName(codecName);
    String fileExtension = TableMetadataParser.getFileExtension(codec);
    String tempMetadataFile = metadataPath(UUID.randomUUID().toString() + fileExtension);
    TableMetadataParser.write(metadata, io().newOutputFile(tempMetadataFile.toString()));

    int nextVersion = (mVersion != null ? mVersion : 0) + 1;
    String finalMetadataFile = metadataFilePath(nextVersion, codec);


    try {
      if (mFileSystem.exists(finalMetadataFile)) {
        throw new CommitFailedException(
            "Version %d already exists: %s", nextVersion, finalMetadataFile);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e,
          "Failed to check if next version exists: " + finalMetadataFile);
    }

    // this rename operation is the atomic commit operation
    renameToFinal(mFileSystem, tempMetadataFile, finalMetadataFile);

    // update the best-effort version pointer
    writeVersionHint(nextVersion);

    mShouldRefresh = true;
  }

  private void writeVersionHint(int versionToWrite) {
    String versionHintFile = versionHintFile();
    // TODO: overwrite has to be on
    try (OutputStream out = mFileSystem.create(versionHintFile)) {
      out.write(String.valueOf(versionToWrite).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.warn("Failed to update version hint", e);
    }
  }

  /**
   * Deletes the file from the file system. Any RuntimeException will be caught and returned.
   *
   * @param path the file to be deleted.
   * @return RuntimeException caught, if any. null otherwise.
   */
  private RuntimeException tryDelete(String path) {
    try {
      io().deleteFile(path);
      return null;
    } catch (RuntimeException re) {
      return re;
    }
  }


  private void renameToFinal(UnderFileSystem fs, String src, String dst) {
    try {
      if (!fs.renameFile(src, dst)) {
        CommitFailedException cfe = new CommitFailedException(
            "Failed to commit changes using rename: %s", dst);
        RuntimeException re = tryDelete(src);
        if (re != null) {
          cfe.addSuppressed(re);
        }
        throw cfe;
      }
    } catch (IOException e) {
      CommitFailedException cfe = new CommitFailedException(e,
          "Failed to commit changes using rename: %s", dst);
      RuntimeException re = tryDelete(src);
      if (re != null) {
        cfe.addSuppressed(re);
      }
      throw cfe;
    }
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
    return metadataPath(fileName);
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }
}
