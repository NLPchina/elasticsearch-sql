## Elasticsearch-SQL

fork from [https://github.com/NLPchina/elasticsearch-sql](https://github.com/NLPchina/elasticsearch-sql)

## features 

All features require ES groovy script enabled.

* Distinct precision_threshold depends on ES or you can specify by second parameters.
  When you have lot of shards, 40000 is consume too much memory. 
* select,groupBy support functions and alias
* more functions support eg. split,floor

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
 
```

    




