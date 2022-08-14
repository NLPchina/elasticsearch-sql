## IMPORTANT

### Since 7.5.0.0, the path `/_sql` is changed to `/_nlpcn/sql`, and the path `/_sql/_explain` is changed to `/_nlpcn/sql/explain`.

----------

## DEPRECATED

### Please note, this project is no longer in active development, and is deprecated, please use official version [x-pack-sql](https://github.com/elastic/elasticsearch/tree/master/x-pack/plugin/sql) and [OpenDistro for Elasticsearch SQL](https://github.com/opendistro-for-elasticsearch/sql) supported by AWS and licensed under Apache 2.

----------

Elasticsearch-SQL
=================

### build status

**6.8.23** [![6.8.23 Build Status](https://travis-ci.com/NLPchina/elasticsearch-sql.svg?branch=elastic6.8.23)](https://travis-ci.com/github/NLPchina/elasticsearch-sql)
**7.17.5** [![7.17.5 Build Status](https://travis-ci.com/NLPchina/elasticsearch-sql.svg?branch=elastic7.17.5)](https://travis-ci.com/github/NLPchina/elasticsearch-sql)
**master** [![master Build Status](https://travis-ci.com/NLPchina/elasticsearch-sql.svg?branch=master)](https://travis-ci.com/github/NLPchina/elasticsearch-sql)

Query elasticsearch using familiar SQL syntax.
You can also use ES functions in SQL.

**Check out our [wiki!](https://github.com/NLPchina/elasticsearch-sql/wiki)**

## explain example 



## Web frontend overview

![Web frontend overview](https://cloud.githubusercontent.com/assets/9518816/5555009/ebe4b53c-8c93-11e4-88ad-96d805cc698f.png)


## SETUP

Install as plugin:
Versions
------------

| elasticsearch version | latest version | remarks                        | branch       |
| --------------------- | -------------  | -----------------------------  | ------------ |
| 1.x	                | 1.7.6          | tested against elastic 1.7.6   | elastic1.x   |
| 2.0.0                 | 2.0.4          | delete commands not supported  | elastic2.0   |
| 2.1.0                 | 2.1.0.2        | delete commands not supported  | elastic2.1   |
| 2.1.1                 | 2.1.1.1        | delete commands not supported  | elastic2.1.1 |
| 2.1.2                 | 2.1.2.0        | delete commands not supported  | elastic2.1.2 |
| 2.2.0                 | 2.2.0.1        | delete commands not supported  | elastic2.2.0 |
| 2.2.1                 | 2.2.1.0        | delete commands not supported  | elastic2.2.1 |
| 2.3.0                 | 2.3.0.0        | delete commands not supported  | elastic2.3.0 |
| 2.3.1                 | 2.3.1.1        | delete commands not supported  | elastic2.3.1 |
| 2.3.2                 | 2.3.2.0        | delete commands not supported  | elastic2.3.2 |
| 2.3.3                 | 2.3.3.0        | delete commands not supported  | elastic2.3.3 |
| 2.3.4                 | 2.3.4.0        | delete commands not supported  | elastic2.3.4 |
| 2.3.5                 | 2.3.5.0        | delete commands not supported  | elastic2.3.5 |
| 2.4.0                 | 2.4.0.1        | delete commands not supported  | elastic2.4.0 |
| 2.4.1                 | 2.4.1.0        | delete commands not supported  | elastic2.4.1 |
| 2.4.2                 | 2.4.2.1        | delete commands not supported  | elastic2.4.2 |
| 2.4.3                 | 2.4.3.0        | delete commands not supported  | elastic2.4.3 |
| 2.4.4                 | 2.4.4.0        | delete commands not supported  | elastic2.4.4 |
| 2.4.5                 | 2.4.5.0        | delete commands not supported  | elastic2.4.5 |
| 2.4.6                 | 2.4.6.0        | delete commands not supported  | elastic2.4.6 |
| 5.0.1                 | 5.0.1.0        | delete commands not supported  | elastic5.0.1 |
| 5.1.1                 | 5.1.1.0        | delete commands not supported  | elastic5.1.1 |
| 5.1.2                 | 5.1.2.0        | delete commands not supported  | elastic5.1.2 |
| 5.2.0                 | 5.2.0.0        | delete commands not supported  | elastic5.2.0 |
| 5.2.1                 | 5.2.1.0        | delete commands not supported  | elastic5.2.1 |
| 5.2.2                 | 5.2.2.0        | delete commands not supported  | elastic5.2.2 |
| 5.3.0                 | 5.3.0.0        | delete commands not supported  | elastic5.3.0 |
| 5.3.1                 | 5.3.1.0        | delete commands not supported  | elastic5.3.1 |
| 5.3.2                 | 5.3.2.0        | delete commands not supported  | elastic5.3.2 |
| 5.3.3                 | 5.3.3.0        | delete commands not supported  | elastic5.3.3 |
| 5.4.0                 | 5.4.0.0        | delete commands not supported  | elastic5.4.0 |
| 5.4.1                 | 5.4.1.0        | delete commands not supported  | elastic5.4.1 |
| 5.4.2                 | 5.4.2.0        | delete commands not supported  | elastic5.4.2 |
| 5.4.3                 | 5.4.3.0        | delete commands not supported  | elastic5.4.3 |
| 5.5.0                 | 5.5.0.1        | delete commands not supported  | elastic5.5.0 |
| 5.5.1                 | 5.5.1.0        | delete commands not supported  | elastic5.5.1 |
| 5.5.2                 | 5.5.2.0        | delete commands not supported  | elastic5.5.2 |
| 5.5.3                 | 5.5.3.0        | delete commands not supported  | elastic5.5.3 |
| 5.6.0                 | 5.6.0.0        | delete commands not supported  | elastic5.6.0 |
| 5.6.1                 | 5.6.1.0        | delete commands not supported  | elastic5.6.1 |
| 5.6.2                 | 5.6.2.0        | delete commands not supported  | elastic5.6.2 |
| 5.6.3                 | 5.6.3.0        | delete commands not supported  | elastic5.6.3 |
| 5.6.4                 | 5.6.4.0        | delete commands not supported  | elastic5.6.4 |
| 5.6.5                 | 5.6.5.0        | delete commands not supported  | elastic5.6.5 |
| 5.6.6                 | 5.6.6.0        |                                | elastic5.6.6 |
| 5.6.7                 | 5.6.7.0        |                                | elastic5.6.7 |
| 5.6.8                 | 5.6.8.0        |                                | elastic5.6.8 |
| 5.6.9                 | 5.6.9.0        |                                | elastic5.6.9 |
| 5.6.10                | 5.6.10.0       |                                | elastic5.6.10|
| 5.6.11                | 5.6.11.0       |                                | elastic5.6.11|
| 5.6.12                | 5.6.12.0       |                                | elastic5.6.12|
| 5.6.13                | 5.6.13.0       |                                | elastic5.6.13|
| 5.6.14                | 5.6.14.0       |                                | elastic5.6.14|
| 5.6.15                | 5.6.15.0       |                                | elastic5.6.15|
| 5.6.16                | 5.6.16.0       |                                | elastic5.6.16|
| 6.0.0                 | 6.0.0.0        |                                | elastic6.0.0 |
| 6.0.1                 | 6.0.1.0        |                                | elastic6.0.1 |
| 6.1.0                 | 6.1.0.0        |                                | elastic6.1.0 |
| 6.1.1                 | 6.1.1.0        |                                | elastic6.1.1 |
| 6.1.2                 | 6.1.2.0        |                                | elastic6.1.2 |
| 6.1.3                 | 6.1.3.0        |                                | elastic6.1.3 |
| 6.1.4                 | 6.1.4.0        |                                | elastic6.1.4 |
| 6.2.0                 | 6.2.0.0        |                                | elastic6.2.0 |
| 6.2.1                 | 6.2.1.0        |                                | elastic6.2.1 |
| 6.2.2                 | 6.2.2.0        |                                | elastic6.2.2 |
| 6.2.3                 | 6.2.3.0        |                                | elastic6.2.3 |
| 6.2.4                 | 6.2.4.0        |                                | elastic6.2.4 |
| 6.3.0                 | 6.3.0.0        |                                | elastic6.3.0 |
| 6.3.1                 | 6.3.1.0        |                                | elastic6.3.1 |
| 6.3.2                 | 6.3.2.0        |                                | elastic6.3.2 |
| 6.4.0                 | 6.4.0.0        |                                | elastic6.4.0 |
| 6.4.1                 | 6.4.1.0        |                                | elastic6.4.1 |
| 6.4.2                 | 6.4.2.0        |                                | elastic6.4.2 |
| 6.4.3                 | 6.4.3.0        |                                | elastic6.4.3 |
| 6.5.0                 | 6.5.0.0        |                                | elastic6.5.0 |
| 6.5.1                 | 6.5.1.0        |                                | elastic6.5.1 |
| 6.5.2                 | 6.5.2.0        |                                | elastic6.5.2 |
| 6.5.3                 | 6.5.3.0        |                                | elastic6.5.3 |
| 6.5.4                 | 6.5.4.0        |                                | elastic6.5.4 |
| 6.6.0                 | 6.6.0.0        |                                | elastic6.6.0 |
| 6.6.1                 | 6.6.1.0        |                                | elastic6.6.1 |
| 6.6.2                 | 6.6.2.0        |                                | elastic6.6.2 |
| 6.7.0                 | 6.7.0.0        |                                | elastic6.7.0 |
| 6.7.1                 | 6.7.1.0        |                                | elastic6.7.1 |
| 6.7.2                 | 6.7.2.0        |                                | elastic6.7.2 |
| 6.8.0                 | 6.8.0.0        |                                | elastic6.8.0 |
| 6.8.1                 | 6.8.1.0        |                                | elastic6.8.1 |
| 6.8.2                 | 6.8.2.0        |                                | elastic6.8.2 |
| 6.8.3                 | 6.8.3.0        |                                | elastic6.8.3 |
| 6.8.4                 | 6.8.4.0        |                                | elastic6.8.4 |
| 6.8.5                 | 6.8.5.0        |                                | elastic6.8.5 |
| 6.8.6                 | 6.8.6.0        |                                | elastic6.8.6 |
| 6.8.7                 | 6.8.7.0        |                                | elastic6.8.7 |
| 6.8.8                 | 6.8.8.0        |                                | elastic6.8.8 |
| 6.8.9                 | 6.8.9.0        |                                | elastic6.8.9 |
| 6.8.10                | 6.8.10.0       |                                | elastic6.8.10|
| 6.8.11                | 6.8.11.0       |                                | elastic6.8.11|
| 6.8.12                | 6.8.12.0       |                                | elastic6.8.12|
| 6.8.13                | 6.8.13.0       |                                | elastic6.8.13|
| 6.8.14                | 6.8.14.0       |                                | elastic6.8.14|
| 6.8.15                | 6.8.15.0       |                                | elastic6.8.15|
| 6.8.16                | 6.8.16.0       |                                | elastic6.8.16|
| 6.8.17                | 6.8.17.0       |                                | elastic6.8.17|
| 6.8.18                | 6.8.18.0       |                                | elastic6.8.18|
| 6.8.19                | 6.8.19.0       |                                | elastic6.8.19|
| 6.8.20                | 6.8.20.0       |                                | elastic6.8.20|
| 6.8.21                | 6.8.21.0       |                                | elastic6.8.21|
| 6.8.22                | 6.8.22.0       |                                | elastic6.8.22|
| 6.8.23                | 6.8.23.0       |                                | elastic6.8.23|
| 7.0.0                 | 7.0.0.0        |                                | elastic7.0.0 |
| 7.0.1                 | 7.0.1.0        |                                | elastic7.0.1 |
| 7.1.0                 | 7.1.0.0        |                                | elastic7.1.0 |
| 7.1.1                 | 7.1.1.0        |                                | elastic7.1.1 |
| 7.2.0                 | 7.2.0.0        |                                | elastic7.2.0 |
| 7.2.1                 | 7.2.1.0        |                                | elastic7.2.1 |
| 7.3.0                 | 7.3.0.0        |                                | elastic7.3.0 |
| 7.3.1                 | 7.3.1.0        |                                | elastic7.3.1 |
| 7.3.2                 | 7.3.2.0        |                                | elastic7.3.2 |
| 7.4.0                 | 7.4.0.0        |                                | elastic7.4.0 |
| 7.4.1                 | 7.4.1.0        |                                | elastic7.4.1 |
| 7.4.2                 | 7.4.2.0        |                                | elastic7.4.2 |
| 7.5.0                 | 7.5.0.0        |                                | elastic7.5.0 |
| 7.5.1                 | 7.5.1.0        |                                | elastic7.5.1 |
| 7.5.2                 | 7.5.2.0        |                                | elastic7.5.2 |
| 7.6.0                 | 7.6.0.0        |                                | elastic7.6.0 |
| 7.6.1                 | 7.6.1.0        |                                | elastic7.6.1 |
| 7.6.2                 | 7.6.2.0        |                                | elastic7.6.2 |
| 7.7.0                 | 7.7.0.0        |                                | elastic7.7.0 |
| 7.7.1                 | 7.7.1.0        |                                | elastic7.7.1 |
| 7.8.0                 | 7.8.0.0        |                                | elastic7.8.0 |
| 7.8.1                 | 7.8.1.0        |                                | elastic7.8.1 |
| 7.9.0                 | 7.9.0.0        |                                | elastic7.9.0 |
| 7.9.1                 | 7.9.1.0        |                                | elastic7.9.1 |
| 7.9.2                 | 7.9.2.0        |                                | elastic7.9.2 |
| 7.9.3                 | 7.9.3.0        |                                | elastic7.9.3 |
| 7.10.0                | 7.10.0.0       |                                | elastic7.10.0|
| 7.10.1                | 7.10.1.0       |                                | elastic7.10.1|
| 7.10.2                | 7.10.2.0       |                                | elastic7.10.2|
| 7.11.0                | 7.11.0.0       |                                | elastic7.11.0|
| 7.11.1                | 7.11.1.0       |                                | elastic7.11.1|
| 7.11.2                | 7.11.2.0       |                                | elastic7.11.2|
| 7.12.0                | 7.12.0.0       |                                | elastic7.12.0|
| 7.12.1                | 7.12.1.0       |                                | elastic7.12.1|
| 7.13.0                | 7.13.0.0       |                                | elastic7.13.0|
| 7.13.1                | 7.13.1.0       |                                | elastic7.13.1|
| 7.13.2                | 7.13.2.0       |                                | elastic7.13.2|
| 7.13.3                | 7.13.3.0       |                                | elastic7.13.3|
| 7.13.4                | 7.13.4.0       |                                | elastic7.13.4|
| 7.14.0                | 7.14.0.0       |                                | elastic7.14.0|
| 7.14.1                | 7.14.1.0       |                                | elastic7.14.1|
| 7.14.2                | 7.14.2.0       |                                | elastic7.14.2|
| 7.15.0                | 7.15.0.0       |                                | elastic7.15.0|
| 7.15.1                | 7.15.1.0       |                                | elastic7.15.1|
| 7.15.2                | 7.15.2.0       |                                | elastic7.15.2|
| 7.16.0                | 7.16.0.0       |                                | elastic7.16.0|
| 7.16.1                | 7.16.1.0       |                                | elastic7.16.1|
| 7.16.2                | 7.16.2.0       |                                | elastic7.16.2|
| 7.16.3                | 7.16.3.0       |                                | elastic7.16.3|
| 7.17.0                | 7.17.0.0       |                                | elastic7.17.0|
| 7.17.1                | 7.17.1.0       |                                | elastic7.17.1|
| 7.17.2                | 7.17.2.0       |                                | elastic7.17.2|
| 7.17.3                | 7.17.3.0       |                                | elastic7.17.3|
| 7.17.4                | 7.17.4.0       |                                | elastic7.17.4|
| 7.17.5                | 7.17.5.0       |                                | elastic7.17.5|

### Elasticsearch 1.x
````
./bin/plugin -u https://github.com/NLPchina/elasticsearch-sql/releases/download/1.7.6/elasticsearch-sql-1.7.6.zip --install sql
````
### Elasticsearch 2.0.0
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.0.4/elasticsearch-sql-2.0.4.zip 
````
### Elasticsearch 2.1.0
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.1.0.2/elasticsearch-sql-2.1.0.2.zip 
````
### Elasticsearch 2.1.1
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.1.1.1/elasticsearch-sql-2.1.1.1.zip 
````
### Elasticsearch 2.1.2
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.1.2.0/elasticsearch-sql-2.1.2.0.zip 
````
### Elasticsearch 2.2.0
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.2.0.1/elasticsearch-sql-2.2.0.1.zip 
````
### Elasticsearch 2.2.1
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.2.1.0/elasticsearch-sql-2.2.1.0.zip 
````
### Elasticsearch 2.3.0
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.3.0.0/elasticsearch-sql-2.3.0.0.zip 
````
### Elasticsearch 2.3.1
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.3.1.1/elasticsearch-sql-2.3.1.1.zip 
````
### Elasticsearch 2.3.2
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.3.2.0/elasticsearch-sql-2.3.2.0.zip 
````
### Elasticsearch 2.3.3
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.3.3.0/elasticsearch-sql-2.3.3.0.zip 
````
### Elasticsearch 2.3.4
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.3.4.0/elasticsearch-sql-2.3.4.0.zip 
````
### Elasticsearch 2.3.5
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.3.5.0/elasticsearch-sql-2.3.5.0.zip 
````
### Elasticsearch 2.4.0
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.4.0.1/elasticsearch-sql-2.4.0.1.zip
````
### Elasticsearch 2.4.1
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.4.1.0/elasticsearch-sql-2.4.1.0.zip
````
### Elasticsearch 2.4.2
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.4.2.1/elasticsearch-sql-2.4.2.1.zip
````
### Elasticsearch 2.4.3
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.4.3.0/elasticsearch-sql-2.4.3.0.zip
````
### Elasticsearch 2.4.4
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.4.4.0/elasticsearch-sql-2.4.4.0.zip
````
### Elasticsearch 2.4.5
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.4.5.0/elasticsearch-sql-2.4.5.0.zip
````

### Elasticsearch 2.4.6
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.4.6.0/elasticsearch-sql-2.4.6.0.zip
````


### Elasticsearch 5.0.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.0.1/elasticsearch-sql-5.0.1.0.zip
````

### Elasticsearch 5.1.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.1.1.0/elasticsearch-sql-5.1.1.0.zip
````

### Elasticsearch 5.1.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.1.2.0/elasticsearch-sql-5.1.2.0.zip
````

### Elasticsearch 5.2.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.2.0.0/elasticsearch-sql-5.2.0.0.zip
````

### Elasticsearch 5.2.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.2.1.0/elasticsearch-sql-5.2.1.0.zip
````

### Elasticsearch 5.2.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.2.2.0/elasticsearch-sql-5.2.2.0.zip
````

### Elasticsearch 5.3.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.3.0.0/elasticsearch-sql-5.3.0.0.zip
````

### Elasticsearch 5.3.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.3.1.0/elasticsearch-sql-5.3.1.0.zip
````

### Elasticsearch 5.3.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.3.2.0/elasticsearch-sql-5.3.2.0.zip
````

### Elasticsearch 5.3.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.3.3.0/elasticsearch-sql-5.3.3.0.zip
````

### Elasticsearch 5.4.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.4.0.0/elasticsearch-sql-5.4.0.0.zip
````

### Elasticsearch 5.4.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.4.1.0/elasticsearch-sql-5.4.1.0.zip
````

### Elasticsearch 5.4.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.4.2.0/elasticsearch-sql-5.4.2.0.zip
````

### Elasticsearch 5.4.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.4.3.0/elasticsearch-sql-5.4.3.0.zip
````

### Elasticsearch 5.5.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.5.0.0/elasticsearch-sql-5.5.0.1.zip
````

### Elasticsearch 5.5.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.5.1.0/elasticsearch-sql-5.5.1.0.zip
````

### Elasticsearch 5.5.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.5.2.0/elasticsearch-sql-5.5.2.0.zip
````

### Elasticsearch 5.5.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.5.3.0/elasticsearch-sql-5.5.3.0.zip
````

### Elasticsearch 5.6.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.0.0/elasticsearch-sql-5.6.0.0.zip
````

### Elasticsearch 5.6.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.1.0/elasticsearch-sql-5.6.1.0.zip
````

### Elasticsearch 5.6.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.2.0/elasticsearch-sql-5.6.2.0.zip
````

### Elasticsearch 5.6.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.3.0/elasticsearch-sql-5.6.3.0.zip
````

### Elasticsearch 5.6.4
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.4.0/elasticsearch-sql-5.6.4.0.zip
````

### Elasticsearch 5.6.5
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.5.0/elasticsearch-sql-5.6.5.0.zip
````

### Elasticsearch 5.6.6
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.6.0/elasticsearch-sql-5.6.6.0.zip
````

### Elasticsearch 5.6.7
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.7.0/elasticsearch-sql-5.6.7.0.zip
````

### Elasticsearch 5.6.8
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.8.0/elasticsearch-sql-5.6.8.0.zip
````

### Elasticsearch 5.6.9
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.9.0/elasticsearch-sql-5.6.9.0.zip
````

### Elasticsearch 5.6.10
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.10.0/elasticsearch-sql-5.6.10.0.zip
````

### Elasticsearch 5.6.11
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.11.0/elasticsearch-sql-5.6.11.0.zip
````

### Elasticsearch 5.6.12
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.12.0/elasticsearch-sql-5.6.12.0.zip
````

### Elasticsearch 5.6.13
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.13.0/elasticsearch-sql-5.6.13.0.zip
````

### Elasticsearch 5.6.14
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.14.0/elasticsearch-sql-5.6.14.0.zip
````

### Elasticsearch 5.6.15
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.15.0/elasticsearch-sql-5.6.15.0.zip
````

### Elasticsearch 5.6.16
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/5.6.16.0/elasticsearch-sql-5.6.16.0.zip
````

### Elasticsearch 6.0.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.0.0.0/elasticsearch-sql-6.0.0.0.zip
````

### Elasticsearch 6.0.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.0.1.0/elasticsearch-sql-6.0.1.0.zip
````

### Elasticsearch 6.1.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.1.0.0/elasticsearch-sql-6.1.0.0.zip
````

### Elasticsearch 6.1.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.1.1.0/elasticsearch-sql-6.1.1.0.zip
````

### Elasticsearch 6.1.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.1.2.0/elasticsearch-sql-6.1.2.0.zip
````

### Elasticsearch 6.1.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.1.3.0/elasticsearch-sql-6.1.3.0.zip
````

### Elasticsearch 6.1.4
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.1.4.0/elasticsearch-sql-6.1.4.0.zip
````

### Elasticsearch 6.2.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.2.0.0/elasticsearch-sql-6.2.0.0.zip
````

### Elasticsearch 6.2.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.2.1.0/elasticsearch-sql-6.2.1.0.zip
````

### Elasticsearch 6.2.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.2.2.0/elasticsearch-sql-6.2.2.0.zip
````

### Elasticsearch 6.2.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.2.3.0/elasticsearch-sql-6.2.3.0.zip
````

### Elasticsearch 6.2.4
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.2.4.0/elasticsearch-sql-6.2.4.0.zip
````

### Elasticsearch 6.3.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.3.0.0/elasticsearch-sql-6.3.0.0.zip
````

### Elasticsearch 6.3.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.3.1.0/elasticsearch-sql-6.3.1.1.zip
````

### Elasticsearch 6.3.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.3.2.0/elasticsearch-sql-6.3.2.0.zip
````

### Elasticsearch 6.4.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.4.0.0/elasticsearch-sql-6.4.0.0.zip
````

### Elasticsearch 6.4.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.4.1.0/elasticsearch-sql-6.4.1.0.zip
````

### Elasticsearch 6.4.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.4.2.0/elasticsearch-sql-6.4.2.0.zip
````

### Elasticsearch 6.4.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.4.3.0/elasticsearch-sql-6.4.3.0.zip
````

### Elasticsearch 6.5.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.5.0.0/elasticsearch-sql-6.5.0.0.zip
````

### Elasticsearch 6.5.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.5.1.0/elasticsearch-sql-6.5.1.0.zip
````

### Elasticsearch 6.5.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.5.2.0/elasticsearch-sql-6.5.2.0.zip
````

### Elasticsearch 6.5.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.5.3.0/elasticsearch-sql-6.5.3.0.zip
````

### Elasticsearch 6.5.4
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.5.4.0/elasticsearch-sql-6.5.4.0.zip
````

### Elasticsearch 6.6.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.6.0.0/elasticsearch-sql-6.6.0.0.zip
````

### Elasticsearch 6.6.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.6.1.0/elasticsearch-sql-6.6.1.0.zip
````

### Elasticsearch 6.6.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.6.2.0/elasticsearch-sql-6.6.2.0.zip
````

### Elasticsearch 6.7.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.7.0.0/elasticsearch-sql-6.7.0.0.zip
````

### Elasticsearch 6.7.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.7.1.0/elasticsearch-sql-6.7.1.0.zip
````

### Elasticsearch 6.7.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.7.2.0/elasticsearch-sql-6.7.2.0.zip
````

### Elasticsearch 6.8.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.0.0/elasticsearch-sql-6.8.0.0.zip
````

### Elasticsearch 6.8.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.1.0/elasticsearch-sql-6.8.1.0.zip
````

### Elasticsearch 6.8.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.2.0/elasticsearch-sql-6.8.2.0.zip
````

### Elasticsearch 6.8.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.3.0/elasticsearch-sql-6.8.3.0.zip
````

### Elasticsearch 6.8.4
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.4.0/elasticsearch-sql-6.8.4.0.zip
````

### Elasticsearch 6.8.5
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.5.0/elasticsearch-sql-6.8.5.0.zip
````

### Elasticsearch 6.8.6
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.6.0/elasticsearch-sql-6.8.6.0.zip
````

### Elasticsearch 6.8.7
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.7.0/elasticsearch-sql-6.8.7.0.zip
````

### Elasticsearch 6.8.8
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.8.0/elasticsearch-sql-6.8.8.0.zip
````

### Elasticsearch 6.8.9
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.9.0/elasticsearch-sql-6.8.9.0.zip
````

### Elasticsearch 6.8.10
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.10.0/elasticsearch-sql-6.8.10.0.zip
````

### Elasticsearch 6.8.11
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.11.0/elasticsearch-sql-6.8.11.0.zip
````

### Elasticsearch 6.8.12
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.12.0/elasticsearch-sql-6.8.12.0.zip
````

### Elasticsearch 6.8.13
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.13.0/elasticsearch-sql-6.8.13.0.zip
````

### Elasticsearch 6.8.14
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.14.0/elasticsearch-sql-6.8.14.0.zip
````

### Elasticsearch 6.8.15
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.15.0/elasticsearch-sql-6.8.15.0.zip
````

### Elasticsearch 6.8.16
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.16.0/elasticsearch-sql-6.8.16.0.zip
````

### Elasticsearch 6.8.17
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.17.0/elasticsearch-sql-6.8.17.0.zip
````

### Elasticsearch 6.8.18
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.18.0/elasticsearch-sql-6.8.18.0.zip
````

### Elasticsearch 6.8.19
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.19.0/elasticsearch-sql-6.8.19.0.zip
````

### Elasticsearch 6.8.20
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.20.0/elasticsearch-sql-6.8.20.0.zip
````

### Elasticsearch 6.8.21
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.21.0/elasticsearch-sql-6.8.21.0.zip
````

### Elasticsearch 6.8.22
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.22.0/elasticsearch-sql-6.8.22.0.zip
````

### Elasticsearch 6.8.23
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/6.8.23.0/elasticsearch-sql-6.8.23.0.zip
````

### Elasticsearch 7.0.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.0.0.0/elasticsearch-sql-7.0.0.0.zip
````

### Elasticsearch 7.0.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.0.1.0/elasticsearch-sql-7.0.1.0.zip
````

### Elasticsearch 7.1.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.1.0.0/elasticsearch-sql-7.1.0.0.zip
````

### Elasticsearch 7.1.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.1.1.0/elasticsearch-sql-7.1.1.0.zip
````

### Elasticsearch 7.2.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.2.0.0/elasticsearch-sql-7.2.0.0.zip
````

### Elasticsearch 7.2.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.2.1.0/elasticsearch-sql-7.2.1.0.zip
````

### Elasticsearch 7.3.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.3.0.0/elasticsearch-sql-7.3.0.0.zip
````

### Elasticsearch 7.3.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.3.1.0/elasticsearch-sql-7.3.1.0.zip
````

### Elasticsearch 7.3.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.3.2.0/elasticsearch-sql-7.3.2.0.zip
````

### Elasticsearch 7.4.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.4.0.0/elasticsearch-sql-7.4.0.0.zip
````

### Elasticsearch 7.4.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.4.1.0/elasticsearch-sql-7.4.1.0.zip
````

### Elasticsearch 7.4.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.4.2.0/elasticsearch-sql-7.4.2.0.zip
````

### Elasticsearch 7.5.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.5.0.0/elasticsearch-sql-7.5.0.0.zip
````

### Elasticsearch 7.5.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.5.1.0/elasticsearch-sql-7.5.1.0.zip
````

### Elasticsearch 7.5.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.5.2.0/elasticsearch-sql-7.5.2.0.zip
````

### Elasticsearch 7.6.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.6.0.0/elasticsearch-sql-7.6.0.0.zip
````

### Elasticsearch 7.6.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.6.1.0/elasticsearch-sql-7.6.1.0.zip
````

### Elasticsearch 7.6.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.6.2.0/elasticsearch-sql-7.6.2.0.zip
````

### Elasticsearch 7.7.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.7.0.0/elasticsearch-sql-7.7.0.0.zip
````

### Elasticsearch 7.7.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.7.1.0/elasticsearch-sql-7.7.1.0.zip
````

### Elasticsearch 7.8.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.8.0.0/elasticsearch-sql-7.8.0.0.zip
````

### Elasticsearch 7.8.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.8.1.0/elasticsearch-sql-7.8.1.0.zip
````

### Elasticsearch 7.9.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.9.0.0/elasticsearch-sql-7.9.0.0.zip
````

### Elasticsearch 7.9.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.9.1.0/elasticsearch-sql-7.9.1.0.zip
````

### Elasticsearch 7.9.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.9.2.0/elasticsearch-sql-7.9.2.0.zip
````

### Elasticsearch 7.9.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.9.3.0/elasticsearch-sql-7.9.3.0.zip
````

### Elasticsearch 7.10.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.10.0.0/elasticsearch-sql-7.10.0.0.zip
````

### Elasticsearch 7.10.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.10.1.0/elasticsearch-sql-7.10.1.0.zip
````

### Elasticsearch 7.10.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.10.2.0/elasticsearch-sql-7.10.2.0.zip
````

### Elasticsearch 7.11.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.11.0.0/elasticsearch-sql-7.11.0.0.zip
````

### Elasticsearch 7.11.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.11.1.0/elasticsearch-sql-7.11.1.0.zip
````

### Elasticsearch 7.11.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.11.2.0/elasticsearch-sql-7.11.2.0.zip
````

### Elasticsearch 7.12.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.12.0.0/elasticsearch-sql-7.12.0.0.zip
````

### Elasticsearch 7.12.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.12.1.0/elasticsearch-sql-7.12.1.0.zip
````

### Elasticsearch 7.13.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.13.0.0/elasticsearch-sql-7.13.0.0.zip
````

### Elasticsearch 7.13.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.13.1.0/elasticsearch-sql-7.13.1.0.zip
````

### Elasticsearch 7.13.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.13.2.0/elasticsearch-sql-7.13.2.0.zip
````

### Elasticsearch 7.13.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.13.3.0/elasticsearch-sql-7.13.3.0.zip
````

### Elasticsearch 7.13.4
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.13.4.0/elasticsearch-sql-7.13.4.0.zip
````

### Elasticsearch 7.14.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.14.0.0/elasticsearch-sql-7.14.0.0.zip
````

### Elasticsearch 7.14.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.14.1.0/elasticsearch-sql-7.14.1.0.zip
````

### Elasticsearch 7.14.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.14.2.0/elasticsearch-sql-7.14.2.0.zip
````

### Elasticsearch 7.15.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.15.0.0/elasticsearch-sql-7.15.0.0.zip
````

### Elasticsearch 7.15.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.15.1.0/elasticsearch-sql-7.15.1.0.zip
````

### Elasticsearch 7.15.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.15.2.0/elasticsearch-sql-7.15.2.0.zip
````

### Elasticsearch 7.16.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.16.0.0/elasticsearch-sql-7.16.0.0.zip
````

### Elasticsearch 7.16.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.16.1.0/elasticsearch-sql-7.16.1.0.zip
````

### Elasticsearch 7.16.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.16.2.0/elasticsearch-sql-7.16.2.0.zip
````

### Elasticsearch 7.16.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.16.3.0/elasticsearch-sql-7.16.3.0.zip
````

### Elasticsearch 7.17.0
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.17.0.0/elasticsearch-sql-7.17.0.0.zip
````

### Elasticsearch 7.17.1
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.17.1.0/elasticsearch-sql-7.17.1.0.zip
````

### Elasticsearch 7.17.2
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.17.2.0/elasticsearch-sql-7.17.2.0.zip
````

### Elasticsearch 7.17.3
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.17.3.0/elasticsearch-sql-7.17.3.0.zip
````

### Elasticsearch 7.17.4
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.17.4.0/elasticsearch-sql-7.17.4.0.zip
````

### Elasticsearch 7.17.5
````
./bin/elasticsearch-plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/7.17.5.0/elasticsearch-sql-7.17.5.0.zip
````

After doing this, you need to restart the Elasticsearch server. Otherwise you may get errors like `Invalid index name [sql], must not start with '']; ","status":400}`.

## Basic Usage

On elasticsearch 1.x / 2.x, visit the elasticsearch-sql web front-end:

````
http://localhost:9200/_plugin/sql/
````

On elasticsearch 5.x/6.x, use [elasticsearch sql site chrome extension](https://github.com/shi-yuan/elasticsearch-sql-site-chrome) (make sure to enable cors on elasticsearch.yml). Alternatively, [download and extract site](https://github.com/NLPchina/elasticsearch-sql/releases/download/5.4.1.0/es-sql-site-standalone.zip), then start the web front-end like this:

```shell
cd site-server
npm install express --save
node node-server.js 
```

* Simple query
````
curl -X GET "localhost:9200/_nlpcn/sql" -H 'Content-Type: application/json' -d'select * from indexName limit 10'
````

* Explain SQL to elasticsearch query DSL
````
curl -X GET "localhost:9200/_nlpcn/sql/explain" -H 'Content-Type: application/json' -d'select * from indexName limit 10'
```` 



## SQL Usage

* Query

        SELECT * FROM bank WHERE age >30 AND gender = 'm'

* Aggregation

        select COUNT(*),SUM(age),MIN(age) as m, MAX(age),AVG(age)
        FROM bank GROUP BY gender ORDER BY SUM(age), m DESC

* Delete

        DELETE FROM bank WHERE age >30 AND gender = 'm'


## Beyond SQL

* Search

        SELECT address FROM bank WHERE address = matchQuery('880 Holmes Lane') ORDER BY _score DESC LIMIT 3
        

* Aggregations

	+ range age group 20-25,25-30,30-35,35-40

			SELECT COUNT(age) FROM bank GROUP BY range(age, 20,25,30,35,40)

	+ range date group by day

			SELECT online FROM online GROUP BY date_histogram(field='insert_time','interval'='1d')

	+ range date group by your config

			SELECT online FROM online GROUP BY date_range(field='insert_time','format'='yyyy-MM-dd' ,'2014-08-18','2014-08-17','now-8d','now-7d','now-6d','now')

* ES Geographic
		
		SELECT * FROM locations WHERE GEO_BOUNDING_BOX(fieldname,100.0,1.0,101,0.0)

* Select type

        SELECT * FROM indexName/type


## SQL Features

*  SQL Select
*  SQL Delete
*  SQL Where
    * can use "case when" in where clause
*  SQL Order By
    * can use "case when" in order by clause
*  SQL Group By
*  SQL AND & OR
*  SQL Like
*  SQL COUNT distinct
*  SQL In
*  SQL Between
*  SQL Aliases
*  SQL Not Null
*  SQL(ES) Date
*  SQL avg()
*  SQL count()
*  SQL last()
*  SQL max()
*  SQL min()
*  SQL sum()
*  SQL Nulls
*  SQL isnull()
*  SQL now()
*  SQL floor
*  SQL split
*  SQL trim
*  SQL log
*  SQL log10
*  SQL substring
*  SQL round
    * eg: round(10.135, 2) --> 10.14
*  SQL sqrt
*  SQL concat_ws
*  SQL union and minus
*  SQL case when
    * can use "in"、"not in" judge in case when clause
    * can execute simple calculation in case when clause, eg : case when 1 = 1 then field_1 + field_2 else 0 end
*  SQL if
    * select if(sex='1','男','女') from t_user;
*  SQL limit
    * can set aggregation bucket size and shard size by setting limit, shardSize = 20 * bucketSize
    * eg: select city,count(*) as user_count from t_user group by city limit 100;
    * on the above example, the bucket size is 100, shard size is 20*100 = 2000
    

## JDBC Support (Experimental feature)

Check details : [JDBC Support](https://github.com/NLPchina/elasticsearch-sql/pull/283) 

## Beyond sql features

*  ES TopHits
*  ES MISSING
*  ES STATS
*  ES GEO_INTERSECTS
*  ES GEO_BOUNDING_BOX
*  ES GEO_DISTANCE
*  ES GEOHASH_GRID aggregation



