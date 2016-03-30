<%--
~ The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
~ (the “License”). You may not use this work except in compliance with the License, which is
~ available at www.apache.org/licenses/LICENSE-2.0
~
~ This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
~ either express or implied, as more fully set forth in the License.
~
~ See the NOTICE file distributed with this work for information regarding copyright ownership.
--%>

<%@ page import="java.util.*" %>
<%@ page import="alluxio.web.*" %>
<%@ page import="static org.apache.commons.lang.StringEscapeUtils.escapeHtml" %>
<%@ page import="static java.net.URLEncoder.encode" %>

<jsp:include page="header-scripts.jsp" />
<script>
  function displayContent() {
    var tmp = document.getElementById("offset").value;
    var base_url = "<%= (request.getAttribute("baseUrl") == null) ? "./browse" : request.getAttribute("baseUrl").toString() %>";
    var href = base_url + "?path=<%= encode(request.getAttribute("currentPath").toString(), "UTF-8") %>&offset=" + tmp;
    if (document.getElementById("relative_end").checked) {
      href += "&end=1";
    }
    window.location.href = href;
  }
  $(document).ready(function() {
    var download_log_file = "<%= (request.getAttribute("downloadLogFile") == null) ? "" : "Local" %>";
    var download_url = "./download" + download_log_file + "?path=<%= encode(request.getAttribute("currentPath").toString(), "UTF-8") %>";
    $("#file-download").attr("href",download_url);
  });
</script>
<div class="container-fluid">
  <jsp:include page="/header" />

  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12">
        <h1 class="text-error">
          <%= request.getAttribute("invalidPathError") %>
        </h1>
        <h4><%= escapeHtml(request.getAttribute("currentPath").toString()) %>: First 5KB from <%= request.getAttribute("viewingOffset") %> in ASCII</h4>
        <textarea class="file-content"><%= request.getAttribute("fileData") %></textarea>
      </div>
    </div>
    <hr>
    <div>
      <span>Display from byte offset </span>
      <input type="text" id="offset" value="<% if(request.getParameter("offset")==null) { %><%= 0 %><% } else { %><%= request.getParameter("offset") %><% } %>"></input>
      <span>  relative to </span>
      <% if(request.getParameter("end")==null) { %>
        <input type="radio" name="rel" id="relative_begin" checked> begin </input>
        <input type="radio" name="rel" id="relative_end"> end </input>
      <% } else { %>
        <input type="radio" name="rel" id="relative_begin"> begin </input>
        <input type="radio" name="rel" id="relative_end" checked> end </input>
      <% } %>
      <a class="btn btn-default" onclick="displayContent();">GO!</a>
      <div>
        <a id="file-download">Download</a>
        <hr>
      </div>
    </div>
    <hr>
    <% if (request.getAttribute("fileBlocks") != null) { %>
      <div>
        <h5>Detailed blocks information (block capacity is <%= request.getAttribute("blockSizeBytes") %> Bytes):</h5>
        <table class="table table-bordered table-striped">
          <tr>
            <th>ID</th>
            <th>Size (Byte)</th>
            <th>In <%= request.getAttribute("highestTierAlias") %></th>
            <th>Locations</th>
          </tr>
          <% for (UIFileBlockInfo masterBlockInfo : ((List<UIFileBlockInfo>) request.getAttribute("fileBlocks"))) { %>
            <tr>
              <td><%= masterBlockInfo.getID() %></td>
              <td><%= masterBlockInfo.getBlockLength() %></td>
              <td>
                <% if (masterBlockInfo.isInTier((String) request.getAttribute("highestTierAlias"))) { %>
                  Yes
                <% } else { %>
                  No
                <% } %>
              </td>
              <td>
                <% Iterator<String> iterator = masterBlockInfo.getLocations().iterator(); %>
                <% while (iterator.hasNext()) { %>
                    <% String location = iterator.next(); %>
                    <a href="http://<%= location %>:<%= request.getAttribute("workerWebPort") %>"><%= location %></a>
                    <% if(iterator.hasNext()) { %>
                      ,
                    <% } %>
                <% } %>
              </td>
            </tr>
          <% } %>
        </table>
      </div>
    <% } %>
  </div>
  <hr>
  <%@ include file="footer.jsp" %>
</div>
