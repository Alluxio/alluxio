var _pre = $("div.highlight > pre.highlight > code");
var endSpan = "</span>"


// Extra processing needs to be done in order to make sure all highlighting is
// encapsulated on the same line
// Sometimes lines may start with whitespace, but then begin with an end span
// use regex to find matches
var spanRegex = new RegExp('^\\s*' + endSpan)

$.each(_pre, function(idx, object) {
    var lines = object.innerHTML.split("\n")

    for(var i = 0; i < lines.length; i++) {
        var matches = lines[i].match(spanRegex)

        if (matches != null && matches.length > 0 && i > 0) {
            // Shouldn't match on i == 0
            // line begins with </span>, so add to the previous line
            lines[i-1] += endSpan
            // Replace the first occurence of endspan with empty string
            lines[i] = lines[i].replace(endSpan, "")
        }
    }
    // Now we can wrap all lines with a span of class="line"
    newlines = lines.map(x => "<span class='line'>" + x + "</span>" )
    newlines.pop() // Remove the last element. It's _always_ an empty line
    object.innerHTML = newlines.join("\n")
})