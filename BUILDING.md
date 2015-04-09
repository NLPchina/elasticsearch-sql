## Building

To build zip release of the plugin, run this command:


````
mvn clean package assembly:single -DskipTests
````

Running this command will create zip release named elasticsearch-sql-{version}.zip under 'target' directory. which can be installed from the file system using elasticsearch plugin bash script:

````
./bin/plugin -u file:///home/omershelef/IdeaProjects/elasticsearch-sql/target/elasticsearch-sql-1.3.2.zip --install sql

````


## Tests

To run the tests, you will need elasticsearch instance running on your local machine. Alternatively you can set the environment variables ES_TEST_HOST and ES_TEST_PORT to point the tests to some other elasticsearch instance instead local machine on default port 9200.
To run the test all you need is running:

````
mvn test

````