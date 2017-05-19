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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AuthenticationMethod.AccessProvider;
import org.javaswift.joss.model.Access;

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

  private static final String AUTH_METHOD = "password";
  private static final int RESPONSE_OK = 201;

  AccountConfig mAccountConfig;

  public KeystoneV3AccessProvider(AccountConfig accountConfig) {
    mAccountConfig = accountConfig;
  }

  @Override
  public Access authenticate() {

    try {
      String requestBody;
      try {
        // Construct request body
        Auth auth = new Auth();
        Identity identity = new Identity();
        identity.setMethods(Arrays.asList(AUTH_METHOD));
        Password password = new Password();
        User user = new User();
        user.setId(mAccountConfig.getUsername());
        user.setPassword(mAccountConfig.getPassword());
        password.setUser(user);
        identity.setPassword(password);
        auth.setIdentity(identity);
        Scope scope = new Scope();
        Project project = new Project();
        project.setId(mAccountConfig.getTenantName());
        scope.setProject(project);
        auth.setScope(scope);
        Request request = new Request();
        request.setAuth(auth);

        requestBody = new ObjectMapper().writeValueAsString(request);
      } catch (JsonProcessingException e) {
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

        bufReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String response = bufReader.readLine();

        String internalURL = response;
        String publicURL = response;
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
      return null;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"auth"})
  public class Request {
    @JsonProperty("auth")
    private Auth auth;

    @JsonProperty("auth")
    public Auth getAuth() {
      return auth;
    }

    @JsonProperty("auth")
    public void setAuth(Auth auth) {
      this.auth = auth;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"identity", "scope"})
  public class Auth {
    @JsonProperty("identity")
    private Identity identity;
    @JsonProperty("scope")
    private Scope scope;

    @JsonProperty("identity")
    public Identity getIdentity() {
      return identity;
    }

    @JsonProperty("identity")
    public void setIdentity(Identity identity) {
      this.identity = identity;
    }

    @JsonProperty("scope")
    public Scope getScope() {
      return scope;
    }

    @JsonProperty("scope")
    public void setScope(Scope scope) {
      this.scope = scope;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"methods", "password"})
  public class Identity {
    @JsonProperty("methods")
    private List<String> methods = null;
    @JsonProperty("password")
    private Password password;

    @JsonProperty("methods")
    public List<String> getMethods() {
      return methods;
    }

    @JsonProperty("methods")
    public void setMethods(List<String> methods) {
      this.methods = methods;
    }

    @JsonProperty("password")
    public Password getPassword() {
      return password;
    }

    @JsonProperty("password")
    public void setPassword(Password password) {
      this.password = password;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"user"})
  public class Password {
    @JsonProperty("user")
    private User user;

    @JsonProperty("user")
    public User getUser() {
      return user;
    }

    @JsonProperty("user")
    public void setUser(User user) {
      this.user = user;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"id"})
  public class Project {
    @JsonProperty("id")
    private String id;

    @JsonProperty("id")
    public String getId() {
      return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
      this.id = id;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"project"})
  public class Scope {
    @JsonProperty("project")
    private Project project;

    @JsonProperty("project")
    public Project getProject() {
      return project;
    }

    @JsonProperty("project")
    public void setProject(Project project) {
      this.project = project;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"id", "password"})
  public class User {
    @JsonProperty("id")
    private String id;
    @JsonProperty("password")
    private String password;

    @JsonProperty("id")
    public String getId() {
      return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
      this.id = id;
    }

    @JsonProperty("password")
    public String getPassword() {
      return password;
    }

    @JsonProperty("password")
    public void setPassword(String password) {
      this.password = password;
    }
  }
}
