package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.PositionReader;
import alluxio.SyncInfo;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.file.options.DescendantType;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.GetStatusOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static alluxio.Constants.DEFAULT_URI_SCHEMA;

public class CachedUnderFileSystem implements UnderFileSystem {

  private static CachedUnderFileSystem instance;
  private final Map<String, UnderFileSystem> ufsCache = new ConcurrentHashMap<>();

  public static CachedUnderFileSystem create() {
    if (instance == null) {
      synchronized (CachedUnderFileSystem.class) {
        if (instance == null) {
          instance = new CachedUnderFileSystem();
        }
      }
    }
    return instance;
  }

  public UnderFileSystem get(String path) {
    AlluxioURI uri = new AlluxioURI(path);
    String scheme = uri.getScheme() == null ? DEFAULT_URI_SCHEMA : uri.getScheme();
    if (!ufsCache.containsKey(scheme)) {
      UnderFileSystem ufs = Factory.create(
          path, UnderFileSystemConfiguration.defaults(Configuration.global()));
      ufsCache.put(scheme, ufs);
    }
    return ufsCache.get(scheme);
  }

  public UnderFileSystem get(AlluxioURI uri) {
    String scheme = uri.getScheme() == null ? DEFAULT_URI_SCHEMA : uri.getScheme();
    if (!ufsCache.containsKey(scheme)) {
      UnderFileSystem ufs = Factory.create(
          uri.getPath(), UnderFileSystemConfiguration.defaults(Configuration.global()));
      ufsCache.put(scheme, ufs);
    }
    return ufsCache.get(scheme);
  }


  @Override
  public void performListingAsync(String path, @Nullable String continuationToken, @Nullable String startAfter, DescendantType descendantType, boolean checkStatus, Consumer<UfsLoadResult> onComplete, Consumer<Throwable> onError) {
    UnderFileSystem ufs = get(path);
    ufs.performListingAsync(path, continuationToken, startAfter, descendantType, checkStatus, onComplete, onError);
  }

  @Override
  public void cleanup() throws IOException {

  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {

  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {

  }

  @Override
  public OutputStream create(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.create(path);
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.create(path, options);
  }

  @Override
  public OutputStream createNonexistingFile(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.createNonexistingFile(path);
  }

  @Override
  public OutputStream createNonexistingFile(String path, CreateOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.createNonexistingFile(path, options);
  }

  @Override
  public boolean deleteDirectory(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.deleteDirectory(path);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.deleteDirectory(path, options);
  }

  @Override
  public boolean deleteExistingDirectory(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.deleteExistingDirectory(path);
  }

  @Override
  public boolean deleteExistingDirectory(String path, DeleteOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.deleteExistingDirectory(path, options);
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.deleteFile(path);
  }

  @Override
  public boolean deleteExistingFile(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.deleteExistingFile(path);
  }

  @Override
  public boolean exists(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.exists(path);
  }

  @Nullable
  @Override
  public Pair<AccessControlList, DefaultAccessControlList> getAclPair(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getAclPair(path);
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getBlockSizeByte(path);
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getDirectoryStatus(path);
  }

  @Override
  public UfsDirectoryStatus getExistingDirectoryStatus(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getExistingDirectoryStatus(path);
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getFileLocations(path);
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getFileLocations(path);
  }

  @Override
  public UfsFileStatus getFileStatus(String path, GetStatusOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getFileStatus(path);
  }

  @Override
  public UfsFileStatus getExistingFileStatus(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getExistingFileStatus(path);
  }

  @Override
  public String getFingerprint(String path) {
    UnderFileSystem ufs = get(path);
    return ufs.getFingerprint(path);
  }

  @Override
  public Fingerprint getParsedFingerprint(String path) {
    UnderFileSystem ufs = get(path);
    return ufs.getParsedFingerprint(path);
  }

  @Override
  public Fingerprint getParsedFingerprint(String path, @Nullable String contentHash) {
    UnderFileSystem ufs = get(path);
    return ufs.getParsedFingerprint(path, contentHash);
  }

  @Override
  public UfsMode getOperationMode(Map<String, UfsMode> physicalUfsState) {
    return null;
  }

  @Override
  public List<String> getPhysicalStores() {
    return null;
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getSpace(path, type);
  }

  @Override
  public UfsStatus getStatus(String path, GetStatusOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getStatus(path, options);
  }

  @Override
  public UfsStatus getExistingStatus(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.getExistingStatus(path);
  }

  @Override
  public String getUnderFSType() {
    return null;
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.isDirectory(path);
  }

  @Override
  public boolean isExistingDirectory(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.isExistingDirectory(path);
  }

  @Override
  public boolean isFile(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.isFile(path);
  }

  @Override
  public boolean isObjectStorage() {
    return false;
  }

  @Override
  public boolean isSeekable() {
    return false;
  }

  @Nullable
  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.listStatus(path);
  }

  @Nullable
  @Override
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.listStatus(path, options);
  }

  @Nullable
  @Override
  public Iterator<UfsStatus> listStatusIterable(String path, ListOptions options, String startAfter, int batchSize) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.listStatusIterable(path, options, startAfter, batchSize);
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.mkdirs(path);
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.mkdirs(path, options);
  }

  @Override
  public InputStream open(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.open(path);
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.open(path, options);
  }

  @Override
  public InputStream openExistingFile(String path) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.openExistingFile(path);
  }

  @Override
  public InputStream openExistingFile(String path, OpenOptions options) throws IOException {
    UnderFileSystem ufs = get(path);
    return ufs.openExistingFile(path, options);
  }

  @Override
  public PositionReader openPositionRead(String path, long fileLength) {
    UnderFileSystem ufs = get(path);
    return ufs.openPositionRead(path, fileLength);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    UnderFileSystem ufs = get(src);
    return ufs.renameDirectory(src, dst);
  }

  @Override
  public boolean renameRenamableDirectory(String src, String dst) throws IOException {
    UnderFileSystem ufs = get(src);
    return ufs.renameRenamableDirectory(src, dst);
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    UnderFileSystem ufs = get(src);
    return ufs.renameFile(src, dst);
  }

  @Override
  public boolean renameRenamableFile(String src, String dst) throws IOException {
    UnderFileSystem ufs = get(src);
    return ufs.renameRenamableFile(src, dst);
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    UnderFileSystem ufs = get(ufsBaseUri);
    return ufs.resolveUri(ufsBaseUri, alluxioPath);
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException {
    UnderFileSystem ufs = get(path);
    ufs.setAclEntries(path, aclEntries);
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    UnderFileSystem ufs = get(path);
    ufs.setMode(path, mode);
  }

  @Override
  public void setOwner(String path, String owner, String group) throws IOException {
    UnderFileSystem ufs = get(path);
    ufs.setOwner(path, owner, group);
  }

  @Override
  public boolean supportsFlush() throws IOException {
    return false;
  }

  @Override
  public boolean supportsActiveSync() {
    return false;
  }

  @Override
  public SyncInfo getActiveSyncInfo() throws IOException {
    return null;
  }

  @Override
  public void startSync(AlluxioURI uri) throws IOException {
    UnderFileSystem ufs = get(uri);
    ufs.startSync(uri);
  }

  @Override
  public void stopSync(AlluxioURI uri) throws IOException {
    UnderFileSystem ufs = get(uri);
    ufs.stopSync(uri);
  }

  @Override
  public boolean startActiveSyncPolling(long txId) throws IOException {
    return false;
  }

  @Override
  public boolean stopActiveSyncPolling() throws IOException {
    return false;
  }

  @Override
  public void close() throws IOException {
  }
}
