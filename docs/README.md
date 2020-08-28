Alluxio Documentation
=====================

Welcome to the Alluxio documentation!

This README will walk you through navigating and building the Alluxio documentation, which is
included here with the Alluxio source code. By building it yourself, you can be sure that you have
the documentation that corresponds to whichever version of Alluxio you currently have checked out of
version control.

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the
documentation yourself.

## Documentation Markdown

We include the Alluxio documentation as part of the source (as opposed to using a hosted wiki, such
as the Github wiki, as the definitive documentation) to enable the documentation to evolve along
with the source code and be captured by revision control (currently git). This way the code
automatically includes the version of the documentation that is relevant regardless of which version
or release you have checked out or downloaded.

In directories like `docs/en` or `docs/cn` you will find text files with a `.md` suffix, formatted
using [Github flavor Markdown syntax](https://help.github.com/articles/basic-writing-and-formatting-syntax/).
You can read those text files directly if you want. Start with `index.md`.

## Generating the Documentation HTML

To make the documentation more visually appealing and easier to navigate, you can generate the HTML
version of the documentation. To do this, you will need to have `Jekyll` installed; the easiest
way to do this is via a Ruby Gem
(see the [jekyll installation instructions](http://jekyllrb.com/docs/installation/)).

Before running `jekyll`, please run mvn to generate Java doc under alluxio root directory.

```console
$ mvn javadoc:javadoc
$ mvn javadoc:aggregate
```

Then go to `docs` directory and use jekyll to build the HTML documentation.

```console
$ cd docs
$ jekyll build
```

This will create a directory called `_site` containing `index.html` as well as the rest of the
HTML files compiled from markdown files.

In addition to generating the site as HTML from the markdown files, jekyll can serve the site via
a web server. To build and run a web server, use the command `jekyll serve` and then visit the site
at [http://localhost:4000](http://localhost:4000).

## Multi-lauguage Support

The markdown files for Alluxio documentation in different languages are stored in separate
directories, e.g., `docs/en` for English documentation and `docs/cn` for Chinese documentation.

## Contributing to the Documentation

We would welcome anyone to contribute to Alluxio Documentation! For more details, please visit [this
page](https://docs.alluxio.io/os/user/stable/en/contributor/Contributor-Getting-Started.html).

## Questions

If you have any question, welcome to ask in our [community](https://www.alluxio.io/community/) or
[Alluxio Community Slack Channel](https://slackin.alluxio.io/)!

