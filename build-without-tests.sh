export JAVA_HOME=$(/usr/libexec/java_home -v13)
mvn clean install -Dmaven.test.skip=true
