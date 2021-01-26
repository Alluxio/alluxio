package alluxio.proxy.s3.tag;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateFilePOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TagHelper {

  private static final String TAG_FOLDER = "/_TAG";
  private static final String FOLDER_OBJECT_TAG_FILE = "_FOLDER_TAG";

  public static void addTag(FileSystem fs, String path, InputStream updateIs)
      throws IOException, AlluxioException {
    final String tagPath = getTagPath(fs, path);

    Map<String, String> tagMap = new HashMap<>();

    final AlluxioURI tagPathURI = new AlluxioURI(tagPath);
    try {
      try (final FileInStream fis = fs.openFile(tagPathURI)) {
        tagMap = new ObjectMapper().readValue(fis, Map.class);
      }
    } catch (FileDoesNotExistException e) {
      // ignore
    }

    final Tagging tagToUpdate = new XmlMapper().readValue(updateIs, Tagging.class);

    for (Tagging.Tag tag : tagToUpdate.getTagSet().getTags()) {
      tagMap.put(tag.getKey(), tag.getValue());
    }

    try (final FileOutStream os = fs.createFile(tagPathURI,
        CreateFilePOptions.newBuilder().setRecursive(true).build())) {
      new ObjectMapper().writeValue(os, tagMap);
    }

  }

  public static Map<String, String> getTags(FileSystem fs, String path) throws IOException, AlluxioException {

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