export BASE_PATH=/mapreduce/wordcount

# Remove output directory
hadoop fs -rmr $BASE_PATH/output

# Run a job with yarn resource manager
yarn jar ./target/wordcount-1.0-SNAPSHOT.jar org.example.WordCount $BASE_PATH/input  $BASE_PATH/output

echo "############################# output data ############################################################"

# Check data
hadoop fs -cat /mapreduce/wordcount/output/part-r-00000 | tail -n 10

