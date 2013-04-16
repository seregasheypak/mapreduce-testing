mapreduce-testing
=================

Stuff for testing mapreduce locally on Java
1. Install maven 3.0.x
2. goto project root
3. run mvn clean install -Pitest
4. enjoy :)
There are several blog post which describe testing approach:
1. Unit tests for mapper, combiner, reducer: http://bigdatapath.com/2013/03/23/test-java-mapreduce-job-locally-without-hadoop-installation-unit-tests/
2. Integration test for mapreduce program: http://bigdatapath.com/2013/04/16/integration-test-for-java-map-reduce-program-using-minimrcluster/