Tachyon
=======

The master branch is in version 0.9.0-SNAPSHOT:

- [Tachyon Website](http://www.tachyon-project.org/) | [Tachyon Latest Release Document](http://www.tachyon-project.org/documentation/) | [Master Branch Document](http://tachyon-project.org/documentation/master/)
- [Contribute to Tachyon](http://tachyon-project.org/documentation/Contributing-to-Tachyon.html) and
[New Contributor's Tasks](https://tachyon.atlassian.net/issues/?jql=project%20%3D%20TACHYON%20AND%20labels%20%3D%20NewContributor%20AND%20status%20%3D%20Open)
  - Please limit 2 tasks per new contributor. Afterwards, try some [beginner tasks](https://tachyon.atlassian.net/issues/?jql=project%20%3D%20TACHYON%20AND%20labels%20%3D%20Beginner%20AND%20status%20%3D%20Open) or [intermediate tasks](https://tachyon.atlassian.net/issues/?jql=project%20%3D%20TACHYON%20AND%20labels%20%3D%20Intermediate%20AND%20status%20%3D%20Open),
  or ask in the [Developer Mailing List](https://groups.google.com/forum/#!forum/tachyon-dev).
- [Releases](http://tachyon-project.org/releases/)
- [Downloads](http://tachyon-project.org/downloads/)
- [JIRA](https://tachyon.atlassian.net/browse/TACHYON)
- [User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/tachyon-users)
- [Bay Area Meetup Group](http://www.meetup.com/Tachyon)

## Building applications with Tachyon

### Dependency Information

#### Apache Maven
```xml
<dependency>
  <groupId>org.tachyonproject</groupId>
  <artifactId>tachyon-client</artifactId>
  <version>0.8.2</version>
</dependency>
```

#### Gradle

```groovy
compile 'org.tachyonproject:tachyon-client:0.8.2'
```

#### Apache Ant
```xml
<dependency org="org.tachyonproject" name="tachyon" rev="0.8.2">
  <artifact name="tachyon-client" type="jar" />
</dependency>
```

#### SBT
```
libraryDependencies += "org.tachyonproject" % "tachyon-client" % "0.8.2"
```

## Contributing to Tachyon

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license the
work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you agree
to license the material under the project's open source license and warrant that you have the legal
authority to do so.
