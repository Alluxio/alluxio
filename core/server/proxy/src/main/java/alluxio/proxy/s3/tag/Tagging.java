package alluxio.proxy.s3.tag;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
 */

@JacksonXmlRootElement(localName = "Tagging")
public class Tagging {
  private TagSet mTagSet;

  public Tagging(Map<String, String> tagMap) {
    final ArrayList<String> keys = new ArrayList<>(tagMap.keySet());
    Collections.sort(keys);

    final ArrayList<Tag> tags = new ArrayList<>();

    for (String key : keys) {
        tags.add(new Tag(key, tagMap.get(key)));
    }

    mTagSet = new TagSet(tags);
  }

  @JacksonXmlProperty(localName = "TagSet")
  public TagSet getTagSet() {
    return mTagSet;
  }

  public class TagSet {
    private List<Tag> mTags;

    private TagSet(List<Tag> tags) {
      mTags = tags;
    }

    @JacksonXmlProperty(localName = "Tag")
    @JacksonXmlElementWrapper(useWrapping = false)
    public List<Tag> getTags() {
      return mTags;
    }

  }

  public class Tag {
    private String mKey;
    private String mValue;

    private Tag(String key, String value) {
      mKey = key;
      mValue = value;
    }

    @JacksonXmlProperty(localName = "Key")
    public String getKey() {
      return mKey;
    }

    @JacksonXmlProperty(localName = "Value")
    public String getValue() {
      return mValue;
    }
  }
}
