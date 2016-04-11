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

<%@ page import="static java.net.URLEncoder.encode" %>

<script type="text/javascript">
  var nTotalFile = <%= request.getAttribute("nTotalFile") %>;

  // default view settings
  var nFilePerPage = 20;
  var nMaxPageShown = 10;

  var base = "<%= (request.getAttribute("baseUrl") == null) ? "./browse" : request.getAttribute("baseUrl").toString() %>";
  var baseUrl = base + "?path=<%= encode(request.getAttribute("currentPath").toString(), "UTF-8") %>";
  var nFilePerPageCookie = "nFilePerPageBrowse";
  var nMaxPageShownCookie = "nMaxPageShownBrowse";

  function constructLink(off, lim) {
    return baseUrl + "&offset=" + off + "&limit=" + lim;
  }