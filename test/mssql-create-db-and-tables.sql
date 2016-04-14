USE master
GO

IF db_id('mapnik_tmp_mssql_db') IS NOT NULL
    DROP DATABASE mapnik_tmp_mssql_db

CREATE DATABASE mapnik_tmp_mssql_db
GO

USE mapnik_tmp_mssql_db
GO

CREATE TABLE [test](
	[gid] [int] IDENTITY(1,1) NOT NULL,[geom] [geometry] NULL, [colbigint] [bigint] NULL, [col_text] [nvarchar](max) NULL, [col-char] [nchar](10) NULL, [col+bool] [bit] NULL, [colnumeric] [numeric](18, 0) NULL, [colsmallint] [smallint] NULL, [colfloat4] [real] NULL, [colfloat8] [float] NULL, [colcharacter] [char](1) NULL, CONSTRAINT [PK_test] PRIMARY KEY CLUSTERED ([gid] ASC) 
)
INSERT INTO test VALUES (geometry::STGeomFromText('POINT(0 0)', 4326), -9223372036854775808, 'I am a point', 'A', 1, 1234567809990001, 0, 0.0, 0.0, 'A');
INSERT INTO test VALUES (geometry::STGeomFromText('POINT(-2 2)', 4326), 9223372036854775807, 'I, too, am a point!', 'B', 0, -123456780999001, 0, 0.0, 0.0, 'A');
INSERT INTO test VALUES (geometry::STGeomFromText('MULTIPOINT(2 1,1 2)', 4326), -1, 'I`m even a MULTI Point', 'Z', 0, 12345678099901, 0, 0.0, 0.0, 'A');
INSERT INTO test VALUES (geometry::STGeomFromText('LINESTRING(0 0,1 1,1 2)', 4326), 0, 'This is a line string', 'ß', 0, -9, 0, 0.0, 0.0, 'A');
INSERT INTO test VALUES (geometry::STGeomFromText('MULTILINESTRING((1 0,0 1,3 2),(3 2,5 4))', 4326), 1, 'multi line string', 'Ü', 1, 0.00001, 0, 0.0, 0.0, 'A');
INSERT INTO test VALUES (geometry::STGeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1, 2 1, 2 2, 1 2,1 1))', 4326), 1, 'polygon', 'Ü', 1, 0.00001, 0, 0.0, 0.0, 'A');
INSERT INTO test VALUES (geometry::STGeomFromText('MULTIPOLYGON(((1 1,3 1,3 3,1 3,1 1),(1 1,2 1,2 2,1 2,1 1)), ((-1 -1,-1 -2,-2 -2,-2 -1,-1 -1)))', 4326), 5432, 'multi ploygon', 'X', 1, 999, 0, 0.0, 0.0, 'A');
INSERT INTO test VALUES (geometry::STGeomFromText('GEOMETRYCOLLECTION(POLYGON((1 1, 2 1, 2 2, 1 2,1 1)),POINT(2 3),LINESTRING(2 3,3 4))', 4326), 8080, 'GEOMETRYCOLLECTION', 'm', 1, 9999, 0, 0.0, 0.0, 'A');

CREATE TABLE test_invalid_id(id numeric PRIMARY KEY, geom geometry);
INSERT INTO test_invalid_id VALUES (1.7, geometry::STGeomFromText('POINT(0 0)', 4326));

CREATE TABLE [test_invalid_multi_col_pk]([id] [int] NOT NULL, [id2] [int] NOT NULL, [geom] [geometry] NULL, CONSTRAINT [PK_test_invalid_multi_col_pk] PRIMARY KEY CLUSTERED ([id] ASC, [id2] ASC))
INSERT INTO test_invalid_multi_col_pk VALUES (1,2,geometry::STGeomFromText('POINT(0 0)', 4326));

CREATE TABLE [test_no_geom_col]([id] [int] IDENTITY(1,1) NOT NULL) ON [PRIMARY]
INSERT INTO test_no_geom_col DEFAULT VALUES;

--simlulate z() function from postgis-vt-util
/*CREATE OR REPLACE FUNCTION z(numeric)
  RETURNS integer AS
$BODY$
begin
    -- Don't bother if the scale is larger than ~zoom level 0
    if $1 > 600000000 then
        return null;
    end if;
    return round(log(2,559082264.028/$1));
end;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;
ALTER FUNCTION z(numeric)
  OWNER TO postgres;*/