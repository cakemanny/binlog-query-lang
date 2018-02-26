BQL - The MySQL Binlog Query Language
=====================================

A language for querying MySQL binary log files.

```sql
-- similar to SQL but no joins
-- has virtual `tables` meta, data, old, new and query
-- unresolved columns are indicated
select meta.timestamp, meta.xid, meta.server_id, data.[0]
  from "REDMOON-3307-bin.000005"
where meta.position >= 4


-- Can stream data
stream new.study, sum(new.pages - old.page)
from "mysql://localhost:3306"
where meta.table_schema = 'nhs_patient_data'
  and meta.table_name = 'confidential_notes'
group by data.study
```

Why
---
Investigation of problems by trawling through the output of mysqlbinlog is slow
and painful, we need a quick and easy way to understand what's been happening
in our binlogs

Virtual Tables
--------------
* meta - binlog event data
* old - Update before image
* new - Update after image
* data - alias for new

Meta Table Columns
------------------
* table\_name
* table\_schema
* query - When using statement-based, or shows creates, drops, etc
* position
* timestamp
* server\_id

How to use
----------
Run the project using sbt. Then connect on port 6032 using, HeidiSQL,
MySQL workbench or the mysql normal client.

```
$ sbt run &
listening on port 6032
```

```
$ mysql --host=127.0.0.1 -P6032
mysql> select meta.timestamp, data.[0] as my_primary_key
    -> where old.[1] <> new.[1] limit 10;
```

Backend
-------
We are currently using shyiko's
[mysql-binlog-connector-java](https://github.com/shyiko/mysql-binlog-connector-java)
for the backend - reading the binary logs. It's well maintained and reliable but
currently we have to scan the entire binlog for each query - so may want to
swap that out when we have the language a bit more stable.


