export JAVA_HOME=$(/usr/libexec/java_home -v13)
mvn clean install -pl parallel-consumer-core -Dmaven.test.skip=true
