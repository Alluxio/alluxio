Tachyon Documentation
=====================

Welcome to the Tachyon documentation!

This README will walk you through navigating and building the Tachyon documentation, which is
included here with the Tachyon source code. By building it yourself, you can be sure that you have
the documentation that corresponds to whichever version of Tachyon you currently have checked out of
version control.

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the
documentation yourself.

## Documentation Markdown

We include the Tachyon documentation as part of the source (as opposed to using a hosted wiki, such
as the Github wiki, as the definitive documentation) to enable the documentation to evolve along
with the source code and be captured by revision control (currently git). This way the code
automatically includes the version of the documentation that is relevant regardless of which version
or release you have checked out or downloaded.

In this directory you will find textfiles formatted using Markdown, with an ".md" suffix. You can
read those text files directly if you want. Start with index.md.

## Generating the Documentation HTML

To make the documentation more visually appealing and easier to navigate, you can generate the HTML
version of the documentation. To do this, you will need to have Jekyll installed; the easiest way to 
do this is via a Ruby Gem (see the [jekyll installation instructions]
(http://jekyllrb.com/docs/installation/)).

Before running `jekyll`, please run `mvn javadoc:javadoc` followed by `mvn javadoc:aggregate` or 
`mvn site:site` to generate JavaDoc.

Next, build the HTML documentation using `SKIP_SCALADOC=1 jekyll build`, which skips building and 
copying over the Scala documentation which can be time-consuming. This will create a directory 
called `_site` containing index.html as well as the rest of the compiled files.

In addition to generating the site as HTML from the markdown files, jekyll can serve the site via
a webserver. To build and run a webserver, use the command `jekyll serve` and then visit the site 
at http://localhost:4000.
