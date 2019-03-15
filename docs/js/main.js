
var _pre = $("div.highlight > pre.highlight > code");

$.each(_pre, function(idx, object) {
    object.innerHTML = "<span class='line'>" + object.innerHTML.split("\n").filter(Boolean).join("</span>\n<span class='line'>") + "</span>";
})
