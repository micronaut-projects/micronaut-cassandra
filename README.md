# Micronaut Cassandra

[![Maven Central](https://img.shields.io/maven-central/v/io.micronaut.cassandra/micronaut-cassandra.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.micronaut.cassandra%22%20AND%20a:%22micronaut-cassandra%22)
[![Build Status](https://github.com/micronaut-projects/micronaut-cassandra/workflows/Java%20CI/badge.svg)](https://github.com/micronaut-projects/micronaut-cassandra/actions)

Micronaut XXX is...

## Documentation

See the [Documentation](https://micronaut-projects.github.io/micronaut-cassandra/latest/guide/) for more information. 

See the [Snapshot Documentation](https://micronaut-projects.github.io/micronaut-cassandra/snapshot/guide/) for the current development docs.

## Snapshots and Releases

Snaphots are automatically published to [JFrog OSS](https://oss.jfrog.org/artifactory/oss-snapshot-local/) using [Github Actions](https://github.com/micronaut-projects/micronaut-cassandra/actions).

See the documentation in the [Micronaut Docs](https://docs.micronaut.io/latest/guide/index.html#usingsnapshots) for how to configure your build to use snapshots.

Releases are published to JCenter and Maven Central via [Github Actions](https://github.com/micronaut-projects/micronaut-cassandra/actions).

A release is performed with the following steps:

* [Edit the version](https://github.com/micronaut-projects/micronaut-cassandra/edit/master/gradle.properties) specified by `projectVersion` in `gradle.properties` to a semantic, unreleased version. Example `1.0.0`
* [Create a new release](https://github.com/micronaut-projects/micronaut-cassandra/releases/new). The Git Tag should start with `v`. For example `v1.0.0`.
* [Monitor the Workflow](https://github.com/micronaut-projects/micronaut-cassandra/actions?query=workflow%3ARelease) to check it passed successfully.
* Celebrate!
