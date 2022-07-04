Reproducer for https://jira.mariadb.org/browse/CONJ-987

Switching the dependency version in the pom.xml demonstrates the change in behaviour between JDBC driver version 2.7.6 and 3.0.6

Requires Docker to run since it uses Testcontainers to setup the database.
