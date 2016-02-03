<script type="text/javascript">
  var nTotalFile = <%= request.getAttribute("inMemoryFileNum") %>;

  // default view settings
  var nFilePerPage = 20;
  var nMaxPageShown = 10;

  var baseUrl = "/memory";
  var nFilePerPageCookie = "nFilePerPageMemory";
  var nMaxPageShownCookie = "nMaxPageShownMemory";

  function constructLink(off, lim) {
    return baseUrl + "?offset=" + off + "&limit=" + lim;
  }

