mvn clean package -Dmaven.test.skip=true;
java -jar -Dspring.batch.job.names=getRunTimeJob ./target/linkedin-spring-batch-0.0.1-SNAPSHOT.jar;
read;
