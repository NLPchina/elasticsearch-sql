Elasticsearch-SQL
=================
**1.X** [![1.X Build Status](https://travis-ci.org/NLPchina/elasticsearch-sql.svg?branch=master)](https://travis-ci.org/NLPchina/elasticsearch-sql) <br>
**2.0.0** [![2.0.0 Build Status](https://travis-ci.org/NLPchina/elasticsearch-sql.svg?branch=elastic2.0)](https://travis-ci.org/NLPchina/elasticsearch-sql)<br>
**2.1.0** [![2.1.0 Build Status](https://travis-ci.org/NLPchina/elasticsearch-sql.svg?branch=elastic2.0)](https://travis-ci.org/NLPchina/elasticsearch-sql)<br>

Query elasticsearch using familiar SQL syntax.
You can also use ES functions in SQL.

**Check out our [wiki!](https://github.com/NLPchina/elasticsearch-sql/wiki)**


## Web frontend overview

![Web frontend overview](https://cloud.githubusercontent.com/assets/9518816/5555009/ebe4b53c-8c93-11e4-88ad-96d805cc698f.png)


## SETUP

Install as plugin:
Versions
------------

| elasticsearch version | latest version | remarks                        | branch     |
| --------------------- | -------------  | -----------------------------  | ---------- |
| 1.X	                | 1.4.7          | tested against elastic 1.4-1.6 | master     |
| 2.0.0                 | 2.0.2          | delete commands not supported  | elastic2.0 |
| 2.1.0                 | 2.1.0          | delete commands not supported  | elastic2.1 |

### Elasticsearch 1.X
````
./bin/plugin -u https://github.com/NLPchina/elasticsearch-sql/releases/download/1.4.7/elasticsearch-sql-1.4.7.zip --install sql
````
### Elasticsearch 2.0.0
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.0.2/elasticsearch-sql-2.0.2.zip 
````
### Elasticsearch 2.1.0
````
./bin/plugin install https://github.com/NLPchina/elasticsearch-sql/releases/download/2.1.0/elasticsearch-sql-2.1.0.zip 
````
After doing this, you need to restart the Elasticsearch server. Otherwise you may get errors like `Invalid index name [sql], must not start with '']; ","status":400}`.

## Basic Usage

* Visit The elasticsearch-sql web front end:
````
http://localhost:9200/_plugin/sql/
````


* Simple query
````
http://localhost:9200/_sql?sql=select * from indexName limit 10
````

* Explain SQL to elasticsearch query DSL
````
http://localhost:9200/_sql/_explain?sql=select * from indexName limit 10
```` 



## SQL Usage

* Query

        SELECT * FROM bank WHERE age >30 AND gender = 'm'

* Aggregation

        select COUNT(*),SUM(age),MIN(age) as m, MAX(age),AVG(age)
        FROM bank GROUP BY gender ORDER BY SUM(age), m DESC

* Delete

        DELETE FROM bank WHERE age >30 AND gender = 'm'


> ###Beyond sql

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
*  SQL Order By
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

## Beyond sql features

*  ES TopHits
*  ES MISSING
*  ES STATS
*  ES GEO_INTERSECTS
*  ES GEO_BOUNDING_BOX
*  ES GEO_DISTANCE
*  ES GEOHASH_GRID aggregation



