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