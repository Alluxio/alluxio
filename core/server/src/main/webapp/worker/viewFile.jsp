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
