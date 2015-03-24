mapnik-mssql
============

A mapnik datasource plugin for Microsoft Sql Server 2012/2014. Based on the PostGIS plugin.

Parameters
----------

| *parameter*       | *value*  | *description* | *default* |
|:------------------|----------|---------------|----------:|
| connection_string     | string       | An odbc connection string. If empty, the parameters "driver", "host", "port", "dbname", "user", "password," and "connect_timeout" will be used. | |
| driver                | string       | name of the odbc driver | SQL Server Native Client 11.0 |
| host                  | string       | host or host\instance | |
| port                  | integer      | used only when there is no instance in server (default 1433) | |
| dbname                | string       | name of the database | |
| user                  | string       | username to use for connecting | |
| password              | string       | user password to use for connecting | |
| connect_timeout       | integer      | timeout is seconds for the connection to take place | 4 |
| persist_connection    | boolean      | choose whether to share the same connection for subsequent queries | true |
| table                 | string       | name of the table to fetch, this can be a sub-query;  subquery has to use syntax of:  '( ) as table'. | |
| geometry_field        | string       | name of the geometry field, in case you have more than one in a single table. This field and the SRID will be deduced from the query in most cases, but may need to be manually specified in some cases.| |
| geometry_table        | string       | name of the table containing the returned geometry; for determining RIDs with subselects | |
| srid                  | integer      | srid of the table, if this is > 0 then fetching data will avoid an extra database query for knowing the srid of the table | 0 |
| extent                | string       | maxextent of the geometries | determined by querying the metadata for the table |
| extent_from_subquery  | boolean      | evaluate the extent of the subquery, this might be a performance issue | false |
| row_limit             | integer      | max number of rows to return when querying data, 0 means no limit | 0 |
| initial_size          | integer      | initial size of the stateless connection pool | 1 |
| max_size              | integer      | max size of the stateless connection pool | 10 |
| simplify_geometries   | boolean      | whether to automatically [reduce input vertices](http://blog.cartodb.com/post/20163722809/speeding-up-tiles-rendering). Only effective when output projection matches (or is similar to) input projection. | false |
| asynchronous_request  | boolean      | Queries are sent asynchronously : while rendering a layer, queries for further layers will run in parallel in the remote server. | false |
| max_async_connection  | integer      | max number of queries for rendering one map in asynchronous mode. Used only when asynchronous_request=true. Default value (1) has no effect. | 1 |

Installation
------------

Copy mssql.input to the mapnik\input directory.

Building instructions
---------------------
### Windows

Requirements for mapnik 2.3:  
 - Visual Studio 2013  
 - Mapnik SDK 2.3

Requirements for mapnik 3.0:  
 - Visual Studio 2015  
 - [Mapnik SDK 3.0](http://mapnik.s3.amazonaws.com/dist/dev/mapnik-win-sdk-14.0-x64-v3.0.0-rc1-57-g2577a6c.7z)

### Linux / OS X (Tested with unixODBC / FreeTDS)
make  
make install

Friendly notice
---------------------
Use at your own risk on production