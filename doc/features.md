## Elasticsearch-SQL

[Elasticsearch-SQL](https://github.com/allwefantasy/elasticsearch-sql/) fork from [https://github.com/NLPchina/elasticsearch-sql](https://github.com/NLPchina/elasticsearch-sql)

visit [interactive web](http://www.nlpcn.org:9999/web/) to feel.

## features 

All features following require ES with groovy script enabled.

* Distinct precision_threshold depends on ES or you can specify by second parameters.
  When you have lot of shards, 40000  consume too much memory. 
* select,groupBy now support functions and field alias 
* nested function is also available,eg.  `split(substring('newtype',0,3),'c')[0]`
* Binary operation support now, eg.  `floor(substring(newtype,0,14)/100)/5)*5`


## functions support
 
 * floor
 * split
 * trim
 * log
 * log10
 * substring
 * round
 * sqrt
 * concat_ws
 * +
 * -
 * * 
 * /
 * %
 
## Example

check Example file:

```
org.nlpcn.es4sql.Test
```

SQLs:

```sql

SELECT newtype as nt  from  twitter2 

SELECT sum(num) as num2,newtype as nt  
from  twitter2 
group by nt  order by num2 

SELECT sum(num_d) as num2,split(newtype,',') as nt  
from  twitter2 
group by nt  
order by num2

SELECT sum(num_d) as num2,floor(num) as nt  
from  twitter2 
group by floor(num),newtype  
order by num2

SELECT split('newtype','b')[1] as nt,sum(num_d) as num2   
from  twitter2 
group by nt

SELECT split(substring('newtype',0,3),'c')[0] as nt,num_d   
from  twitter2 
group by nt

SELECT trim(newtype) as nt 
from  twitter2


SELECT floor(floor(substring(time,0,14)/100)/5)*5 as nt,
count(distinct(mid)) as cvalue 
FROM twitter2  
where ty='buffer' and day='20160815' and domain='baidu.com' 
group by nt 
order by cvalue 
 
```