alluxio
=======

The master branch is in version 0.9.0-SNAPSHOT:

- [alluxio Website](http://www.alluxio.org/) | [Alluxio Latest Release Document](http://www.alluxio.org/documentation/) | [Master Branch Document](http://alluxio.org/documentation/master/)
- [Contribute to Alluxio](http://alluxio.org/documentation/Contributing-to-Alluxio.html) and
[New Contributor Tasks](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20labels%20%3D%20NewContributor%20AND%20status%20%3D%20Open)
  - Please limit 2 tasks per new contributor. Afterwards, try some [beginner tasks](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20labels%20%3D%20Beginner%20AND%20status%20%3D%20Open) or [intermediate tasks](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20labels%20%3D%20Intermediate%20AND%20status%20%3D%20Open),
  or ask in the [Developer Mailing List](https://groups.google.com/forum/#!forum/alluxio-dev).
- [Releases](http://alluxio.org/releases/)
- [Downloads](http://alluxio.org/downloads/)
- [JIRA](https://alluxio.atlassian.net/browse/ALLUXIO)
- [User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)
- [Bay Area Meetup Group](http://www.meetup.com/Alluxio)

## Building applications with Alluxio

### Dependency Information

#### Apache Maven
```xml
<dependency>
  <groupId>org.alluxio.alluxio</groupId>
  <artifactId>alluxio-client</artifactId>
  <version>1.0.0</version>
</dependency>
```

#### Gradle

```groovy
compile 'org.alluxio.alluxio:alluxio-client:1.0.0'
```

#### Apache Ant
```xml
<dependency org="org.alluxio.alluxio" name="alluxio" rev="1.0.0">
  <artifact name="alluxio-client" type="jar" />
</dependency>
```

#### SBT
```
libraryDependencies += "org.alluxioalluxio" % "alluxio-client" % "1.0.0"
```

## Contributing to Alluxio

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license the
work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you agree
to license the material under the project's open source license and warrant that you have the legal
authority to do so.
