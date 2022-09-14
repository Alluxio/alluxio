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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.junit.Assert;
import org.junit.Test;

public class S3RestServiceHandlerTest {
  private final AlluxioConfiguration mConf = Configuration.global();

  @Test
  public void userFromAuthorization() throws Exception {
    try {
      S3AuthenticationFilter.getUserFromAuthorization("AWS-SHA256-HMAC Credential=/asd", mConf);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof S3Exception);
    }
    try {
      assertNull(S3AuthenticationFilter.getUserFromAuthorization("", mConf));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof S3Exception);
    }
    assertEquals("test", S3AuthenticationFilter.getUserFromAuthorization(
        "AWS-SHA256-HMAC Credential=test/asd", mConf));
  }

  @Test
  public void testDeleteObjectReq() throws Exception {
    String content = "<Delete><Quiet>true</Quiet><Object><Key>new-file-001.snappy"
        + ".parquet/_temporary/</Key></Object><Object><Key>new-file-001.snappy.parquet/</Key>"
        + "</Object></Delete>";
    XmlMapper mapper = new XmlMapper();
    DeleteObjectsRequest r =
        mapper.readerFor(DeleteObjectsRequest.class).readValue(content.getBytes());
    String o = mapper.writerFor(DeleteObjectsRequest.class).writeValueAsString(r);
    assertTrue(r.getQuiet());
    assertEquals(2, r.getToDelete().size());
    assertEquals(o, content);
  }

  @Test
  public void testDeleteObjectResp() throws Exception {
    String content = "<DeleteResult><Deleted><DeleteMarker>boolean</DeleteMarker>"
        + "<DeleteMarkerVersionId>string</DeleteMarkerVersionId>"
        + "<Key>string</Key><VersionId>string</VersionId></Deleted><Error><Code>string</Code>"
        + "<Key>string</Key><Message>string</Message><VersionId>string</VersionId></Error>"
        + "</DeleteResult>";
    XmlMapper mapper = new XmlMapper();
    DeleteObjectsResult r =
        mapper.readerFor(DeleteObjectsResult.class).readValue(content.getBytes());
    String o = mapper.writerFor(DeleteObjectsResult.class).writeValueAsString(r);
    assertTrue(r.getDeleted().size() > 0);
    assertTrue(r.getErrored().size() > 0);
  }
}
