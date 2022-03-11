# th2 lib template library

This is the template project for creating custom libraries for working with th2, which contains helpful util functions for different exchange protocols. <br>

## How to transform the template
1. Edit package name (`com.exactpro.th2.lib.template` in template). It should correspond to the project name (e.g., `com.exactpro.th2.lib.protocol`).
2. Place your custom code files in the package.
3. Edit `release_version` and `vcs_url` properties in `gradle.properties` file.
4. Edit `rootProject.name` variable in `settings.gradle` file. This will be the name of the Java package.
5. Edit `README.md` file according to the new project.

## How to maintain a project
1. Perform the necessary changes.
2. Update the package version of Java in `gradle.properties` file.
3. Commit everything.

## How to run project

### Java
If you wish to manually create and publish a package for Java, run the following command:
```
gradle --no-daemon clean build publish artifactoryPublish \
       -Purl=${URL} \ 
       -Puser=${USER} \
       -Ppassword=${PASSWORD}
```
`URL`, `USER` and `PASSWORD` are parameters for publishing.
