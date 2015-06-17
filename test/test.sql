USE master
GO

IF db_id('mapnik_tmp_mssql_db') IS NOT NULL
    DROP DATABASE mapnik_tmp_mssql_db

CREATE DATABASE mapnik_tmp_mssql_db
GO

USE mapnik_tmp_mssql_db
GO

CREATE TABLE table1 
(
    _bigint BIGINT,
	_bit BIT,
	_decimal DECIMAL(9,2),
	_int INT,
	_money MONEY,
	_numeric NUMERIC(9,2),
	_smallint SMALLINT,
	_smallmoney SMALLMONEY,
	_tinyint TINYINT,
	_float FLOAT,
	_real REAL,
	_date DATE,
	_datetime2 DATETIME2,
	_datetime DATETIME,
	_datetimeoffset DATETIMEOFFSET,
	_smalldatetime SMALLDATETIME,
	_time TIME,
	_char CHAR,
	_text TEXT,
	_varchar VARCHAR(255),
	_nchar NCHAR,
	_ntext NTEXT,
	_nvarchar NVARCHAR(255),
	_binary BINARY,
	_image IMAGE,
	_varbinary VARBINARY(255),
	_uniqueidentifier UNIQUEIDENTIFIER,
	_null INT,
    geom GEOMETRY
)
GO

INSERT INTO [dbo].[table1]
           ([_bigint]
           ,[_bit]
           ,[_decimal]
           ,[_int]
           ,[_money]
           ,[_numeric]
           ,[_smallint]
           ,[_smallmoney]
           ,[_tinyint]
           ,[_float]
           ,[_real]
           ,[_date]
           ,[_datetime2]
           ,[_datetime]
           ,[_datetimeoffset]
           ,[_smalldatetime]
           ,[_time]
           ,[_char]
           ,[_text]
           ,[_varchar]
           ,[_nchar]
           ,[_ntext]
           ,[_nvarchar]
           ,[_binary]
           ,[_image]
           ,[_varbinary]
           ,[_uniqueidentifier]
		   ,[_null]
           ,[geom])
     VALUES
           (2147483648
           ,1
           ,1.25
           ,1
           ,1.25
           ,1.25
           ,1
           ,1.25
           ,1
           ,1.25
           ,1.25
           ,'2000-01-01'
           ,'2000-01-01'
           ,'2000-01-01'
           ,'2000-01-01 12:00:00 +01:00'
           ,'2000-01-01'
           ,'12:00:00'
           ,'a'
           ,'text'
           ,'text'
           ,'a'
           ,'text'
           ,'text'
           ,0x2A
           ,0x2A
           ,0x2A
           ,'BE62A766-2614-42F9-A26E-74DDB9065977'
           ,NULL
           ,GEOMETRY::STGeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', 900913))
GO

