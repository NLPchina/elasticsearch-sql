## Elasticsearch-SQL

fork from [https://github.com/NLPchina/elasticsearch-sql](https://github.com/NLPchina/elasticsearch-sql)

## features 

All features require ES groovy script enabled.

* Distinct precision_threshold depends on ES or you can specify by second parameters.
  When you have lot of shards, 40000 is consume too much memory. 
* select,groupBy support functions and alias
* nested function available,eg.  `split(substring('newtype',0,3),'c')[0]`


## functions support
 
 * floor
 * split
 * trim
 * log
 * log10
 * substring
 * round
 * sqrt

## Example

check Example file:

```
org.nlpcn.es4sql.Test
```

SQLs:

```sql

SELECT newtype as nt  from  twitter2 

SELECT sum(num) as num2,newtype as nt  from  twitter2 group by nt  order by num2 

SELECT sum(num_d) as num2,split(newtype,',') as nt  from  twitter2 group by nt  order by num2

SELECT sum(num_d) as num2,floor(num) as nt  from  twitter2 group by floor(num),newtype  order by num2

SELECT split('newtype','b')[1] as nt,sum(num_d) as num2   from  twitter2 group by nt

SELECT split(substring('newtype',0,3),'c')[0] as nt,num_d   from  twitter2 group by nt

SELECT trim(newtype) as nt from  twitter2
 
```

    




