# Building and updating the CodeStyle
IntelliJ supports importing CodeStyle settings but only when this CodeStyle is packaged as a jar. The provided "import" script in this directory compiles the necessary files into a jar that can then be imported into IntelliJ.

We use this approach, rather than storing the CodeStyle jar itself, since it allows changes to CodeStyle settings to be tracked through GitHub.

The complementary script, "update", allows you to specify an exported IntelliJ file, and unpack its contents locally. This can therefore be used to make changes to IntelliJ configurations, export them as a JAR, and then update the files stored in Git to be submitted for review.
