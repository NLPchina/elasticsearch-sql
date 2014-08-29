elasticsearch-sql
=================

elasticsearch-sql旨在将elasticsearch索引库兼容关系型数据库sql查询，开发这个项目的初衷是由于elasticsearch的学习成本比较高，并且文档较少，这个项目开发成功后可以降低初学者的学习成本。


# SETUP 

* maven
  > 还木有
* down jar
  > 还木有

# Simple Case

> you can use it like database and beyond sql .
> 
> * Query
> 
> 	select * from blank where age >30 and gender ="m" ;
> 	
>
> * Aggregation
> 
> 	select count(*),sum(age),min(age) as m,max(age),avg(age) from bank group by gender order by sum(age),m desc











# NOW
> 列出已经实现的功能

* sum
* max

# FEATURE
> 列出打算将要做的功能
*  SQL select
*  SQL distinct
*  SQL where
*  SQL AND & OR
*  SQL Order By
*  SQL insert
*  SQL update
*  SQL delete
*  SQL Top
*  SQL Like
*  SQL 通配符
*  SQL In
*  SQL Between
*  SQL Aliases
*  SQL Join
*  SQL Inner Join
*  SQL Left Join
*  SQL Right Join
*  SQL Full Join
*  SQL Union
*  SQL Select Into
*  SQL Create DB
*  SQL Create Table
*  SQL Constraints
*  SQL Not Null
*  SQL Unique
*  SQL Primary Key
*  SQL Foreign Key
*  SQL Check
*  SQL Default
*  SQL Create Index
*  SQL Drop
*  SQL Alter
*  SQL Increment
*  SQL View
*  SQL Date
*  SQL Nulls
*  SQL isnull()
*  SQL functions
*  SQL avg()
*  SQL count()
*  SQL first()
*  SQL last()
*  SQL max()
*  SQL min()
*  SQL sum()
*  SQL Group By
*  SQL Having
*  SQL ucase()
*  SQL lcase()
*  SQL mid()
*  SQL len()
*  SQL round()
*  SQL now()
*  SQL format()
