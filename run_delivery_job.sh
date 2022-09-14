CURRENT_DATE=`date '+%Y/%m/%d'`
LESSON=$(basename $PWD)
mvn clean package -Dmaven.test.skip=true;
java -jar -Dspring.batch.job.names=deliverPackageJob ./target/linkedin-spring-batch-0.0.1-SNAPSHOT.jar "item=$1" "run.date(date)=$CURRENT_DATE" "lesson=$LESSON" "gotLostProbability(double)=$2";
read;
