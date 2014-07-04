# Copy over the JavaDoc for all projects to api/java.
require 'fileutils'
include FileUtils

puts "Copy over the JavaDoc for all projects to api/java"

source = "../core/target/site/apidocs"
dest = "api/java"

puts "Making directory " + dest
mkdir_p dest

puts "cp -r " + source + "/. " + dest
cp_r(source + "/.", dest)

cd("..")
