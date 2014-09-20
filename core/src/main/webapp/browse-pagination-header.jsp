<script src="js/cookies.min.js" type="text/javascript"></script>
<script type="text/javascript">
  var nTotalFile = <%= request.getAttribute("nTotalFile") %>;

  // default view settings
  var nFilePerPage = 20;
  var nMaxPageShown = 10;

  var baseUrl = "/browse?path=<%= encode(request.getAttribute("currentPath").toString(), "UTF-8") %>";
  var nFilePerPageCookie = "nFilePerPageBrowse";
  var nMaxPageShownCookie = "nMaxPageShownBrowse";

  function constructLink(off, lim) {
    return baseUrl + "&offset=" + off + "&limit=" + lim;
  }