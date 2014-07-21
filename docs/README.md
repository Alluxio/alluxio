Welcome to the Tachyon documentation!

This README will walk you through navigating and building the Tachyon documentation, which is
included here with the Tachyon source code. By building it yourself, you can be sure that you have
the documentation that corresponds to whichever version of Tachyon you currently have checked out of
version control.

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the
documentation yourself.

## Generating the Documentation HTML

We include the Tachyon documentation as part of the source (as opposed to using a hosted wiki, such
as the Github wiki, as the definitive documentation) to enable the documentation to evolve along
with the source code and be captured by revision control (currently git). This way the code
automatically includes the version of the documentation that is relevant regardless of which version
or release you have checked out or downloaded.

In this directory you will find textfiles formatted using Markdown, with an ".md" suffix. You can
read those text files directly if you want. Start with index.md.

Before running `jekyll`, please run `mvn javadoc:javadoc` or `mvn site:site` to generate Java Doc.

To make things quite a bit prettier and make the links easier to follow, generate the html version
of the documentation based on the src directory by running `jekyll` in the docs directory. Use the
command `SKIP_SCALADOC=1 jekyll` to skip building and copying over the scaladoc which can be
time-consuming. To use the `jekyll` command, you will need to have Jekyll installed; the easiest way
to do this is via a Ruby Gem (see the
[jekyll installation instructions](https://github.com/mojombo/jekyll/wiki/install)). This will
create a directory called `_site` containing index.html as well as the rest of the compiled files.
Read more about Jekyll at https://github.com/mojombo/jekyll/wiki.

In addition to generating the site as html from the markdown files, jekyll can serve up the site via
a webserver. To build and run a webserver use the command `jekyll --server` which (currently) runs
the webserver on port 4000, then visit the site at http://localhost:4000.
