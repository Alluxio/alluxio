# Copy over the JavaDoc for all projects to api/java.
require 'fileutils'
include FileUtils

puts "Copy over the JavaDoc for all projects to api/java"

source = "../target/site/apidocs"
dest = "api/java"

puts "Making directory " + dest
mkdir_p dest

if !File.directory?(source)
  puts "WARNING: " + source + " not found, continuing without javadoc"
else
  puts "cp -r " + source + "/. " + dest
  cp_r(source + "/.", dest)
end

cd("..")
