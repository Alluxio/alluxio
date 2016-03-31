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

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <jsp:include page="../header-links.jsp" />
</head>
<title>Alluxio</title>
<body>
  <% request.setAttribute("useWorkerHeader", "1"); %>
  <jsp:include page="../viewFile-body.jsp" />
</body>
</html>
