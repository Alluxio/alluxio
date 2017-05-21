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

package alluxio.underfs.swift;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AuthenticationMethod.AccessProvider;
import org.javaswift.joss.model.Access;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

/**
 * Custom {@link AccessProvider} for Keystone V3 authentication.
 */
public class KeystoneV3AccessProvider implements AccessProvider {
  private static final Logger LOG = LoggerFactory.getLogger(KeystoneV3AccessProvider.class);

  private static final String AUTH_METHOD = "password";
  private static final int RESPONSE_OK = 201;

  private AccountConfig mAccountConfig;

  /**
   * Create a new instance of {@link KeystoneV3AccessProvider}.
   *
   * @param accountConfig account credentials
   */
  public KeystoneV3AccessProvider(AccountConfig accountConfig) {
    mAccountConfig = accountConfig;
  }

  @Override
  public Access authenticate() {

    try {
      String requestBody;
      try {
        // Construct request body
        KeystoneV3Request request =
            new KeystoneV3Request(new Auth(
                new Identity(Arrays.asList(AUTH_METHOD),
                    new Password(
                        new User(mAccountConfig.getUsername(), mAccountConfig.getPassword()))),
                new Scope(new Project(mAccountConfig.getTenantName()))));
        requestBody = new ObjectMapper().writeValueAsString(request);
      } catch (JsonProcessingException e) {
        LOG.error("Error processing JSON request: {}", e.getMessage());
        return null;
      }

      HttpURLConnection connection = null;
      BufferedReader bufReader = null;
      try {
        // Send request
        connection = (HttpURLConnection) new URL(mAccountConfig.getAuthUrl()).openConnection();
        connection.setDoOutput(true);
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Content-Type", "application/json");
        OutputStream output = connection.getOutputStream();
        output.write(requestBody.toString().getBytes());

        // Parse response
        if (connection.getResponseCode() != RESPONSE_OK) {
          return null;
        }
        String token = connection.getHeaderField("X-Subject-Token");

        // Parse response body
        bufReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String responseBody = bufReader.readLine();
        KeystoneV3Response response;
        try {
          response = new ObjectMapper().readerFor(KeystoneV3Response.class).readValue(responseBody);
        } catch (JsonProcessingException e) {
          LOG.error("Error processing JSON response: {}", e.getMessage());
          return null;
        }
        // Find endpoints
        String internalURL = null;
        String publicURL = null;
        for (Catalog catalog : response.mToken.mCatalog) {
          if (catalog.mName.equals("swift") && catalog.mType.equals("object-store")) {
            for (Endpoint endpoint : catalog.mEndpoints) {
              if (endpoint.mRegion.equals(mAccountConfig.getPreferredRegion())) {
                if (endpoint.mInterface.equals("public")) {
                  publicURL = endpoint.mUrl;
                } else if (endpoint.mInterface.equals("internal")) {
                  internalURL = endpoint.mUrl;
                }
              }
            }
          }
        }
        // Construct access object
        KeystoneV3Access access = new KeystoneV3Access(internalURL,
            mAccountConfig.getPreferredRegion(), publicURL, token);
        return access;
      } finally {
        // Cleanup
        if (bufReader != null) {
          bufReader.close();
        }
        if (connection != null) {
          connection.disconnect();
        }
      }
    } catch (IOException e) {
      // Unable to authenticate
      LOG.error("Exception authenticating using KeystoneV3 {}", e.getMessage());
      return null;
    }
  }

  /** Classes for creating authentication JSON request. */
  @JsonPropertyOrder({"auth"})
  private class KeystoneV3Request {
    @JsonProperty("auth")
    public Auth mAuth;

    public KeystoneV3Request(Auth auth) {
      mAuth = auth;
    }
  }

  @JsonPropertyOrder({"identity", "scope"})
  private class Auth {
    @JsonProperty("identity")
    public Identity mIdentity;
    @JsonProperty("scope")
    public Scope mScope;

    public Auth(Identity identity, Scope scope) {
      mIdentity = identity;
      mScope = scope;
    }
  }

  @JsonPropertyOrder({"methods", "password"})
  private class Identity {
    @JsonProperty("methods")
    public List<String> mMethods = null;
    @JsonProperty("password")
    public Password mPassword;

    public Identity(List<String> methods, Password password) {
      mMethods = methods;
      mPassword = password;
    }
  }

  @JsonPropertyOrder({"user"})
  private class Password {
    @JsonProperty("user")
    public User mUser;

    public Password(User user) {
      mUser = user;
    }
  }

  @JsonPropertyOrder({"id"})
  private class Project {
    @JsonProperty("id")
    public String mId;

    public Project(String id) {
      mId = id;
    }
  }

  @JsonPropertyOrder({"project"})
  private class Scope {
    @JsonProperty("project")
    public Project mProject;

    public Scope(Project project) {
      mProject = project;
    }
  }

  @JsonPropertyOrder({"id", "password"})
  private class User {
    @JsonProperty("id")
    public String mId;
    @JsonProperty("password")
    public String mPassword;

    public User(String id, String password) {
      mId = id;
      mPassword = password;
    }
  }

  /** Classes for parsing authentication JSON response. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class KeystoneV3Response {
    public Token mToken;

    @JsonCreator
    public KeystoneV3Response(@JsonProperty("token") Token token) {
      mToken = token;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class Token {
    public List<Catalog> mCatalog = null;

    @JsonCreator
    public Token(@JsonProperty("catalog") List<Catalog> catalog) {
      mCatalog = catalog;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class Catalog {
    public List<Endpoint> mEndpoints;
    public String mType;
    public String mName;

    @JsonCreator
    public Catalog(@JsonProperty("endpoints") List<Endpoint> endpoints,
        @JsonProperty("type") String type, @JsonProperty("name") String name) {
      mEndpoints = endpoints;
      mType = type;
      mName = name;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class Endpoint {
    public String mUrl;
    public String mRegion;
    public String mInterface;

    @JsonCreator
    public Endpoint(@JsonProperty("url") String url, @JsonProperty("region") String region,
        @JsonProperty("interface") String inter) {
      mUrl = url;
      mRegion = region;
      mInterface = inter;
    }
  }
}
