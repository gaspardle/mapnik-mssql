/*****************************************************************************
*
* This file is part of Mapnik (c++ mapping toolkit)
*
* Copyright (C) 2011 Artem Pavlenko
*
* This library is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License as published by the Free Software Foundation; either
* version 2.1 of the License, or (at your option) any later version.
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this library; if not, write to the Free Software
* Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*
*****************************************************************************/

#ifndef MSSQL_RESULTSET_HPP
#define MSSQL_RESULTSET_HPP


#ifdef _WINDOWS
#define NOMINMAX
#include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>


class IResultSet
{
public:
	//virtual IResultSet& operator=(const IResultSet& rhs) = 0;
	virtual ~IResultSet() {}
	virtual void close() = 0;
	virtual int getNumFields() const = 0;
	virtual bool next() = 0;
	virtual const std::string getFieldName(int index) const = 0;
	virtual int getFieldLength(int index) const = 0;
	virtual int getFieldLength(const char* name) const = 0;
	virtual int getTypeOID(int index) const = 0;
	virtual int getTypeOID(const char* name) const = 0;
	virtual bool isNull(int index) const = 0;
	virtual const int getInt(int index) const = 0;
	virtual const double getDouble(int index) const = 0;
	virtual const float getFloat(int index) const = 0;
	virtual const std::string getString(int index) const = 0;
	virtual const std::vector<const char> getBinary(int index) const = 0;
	//virtual const char* getValue(int index) const = 0;
	//virtual const char* getValue(const char* name) const = 0;
};

class ResultSet : public IResultSet, private mapnik::noncopyable
{
public:
	ResultSet(SQLHANDLE res)
		: res_(res),
		pos_(-1)
	{
		//numTuples_ = PQntuples(res_);        
	}

	virtual void close()
	{
		//PQclear(res_);
		// SQLDisconnect(res_);
		SQLFreeHandle(SQL_HANDLE_STMT, res_);
		res_ = 0;
	}

	virtual ~ResultSet()
	{
		SQLFreeHandle(SQL_HANDLE_STMT, res_);
	}

	virtual int getNumFields() const
	{
		SQLSMALLINT column_count;

		if (SQLNumResultCols(res_, &column_count) < 0){
			throw mapnik::datasource_exception("resultset getNumFields error");
			//return 0;
		}
		return column_count;
	}

	int pos() const
	{
		return pos_;
	}

	int size() const
	{
		return numTuples_;
	}

	virtual bool next()
	{
		//return (++pos_ < numTuples_);
		
		++pos_;
		SQLRETURN retcode;
		retcode = SQLFetch(res_);
		if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO){
			return true;
		}
		else if (retcode == SQL_NO_DATA || retcode == SQL_STILL_EXECUTING /*XXX i have no idea*/){
			return false;
		}
		else{
			return false;
			//XXX
			SQLWCHAR  sqlstate[6];
			SQLWCHAR  message[SQL_MAX_MESSAGE_LENGTH];
			SQLINTEGER  NativeError;
			SQLSMALLINT   i, MsgLen;
			SQLRETURN rc2 = SQLGetDiagRec(SQL_HANDLE_STMT, res_, 1, sqlstate, &NativeError,
				message, sizeof(message), &MsgLen);

			throw mapnik::datasource_exception("resultset next error");
		}

	}

	virtual const std::string getFieldName(int index) const
	{
		char fname[256];
		SQLSMALLINT name_length;
		SQLRETURN retcode;

		retcode = SQLColAttributeA(
			res_,
			index + 1,                    /* the Column number */
			SQL_DESC_NAME,        /* the field identifier, = 1011 */
			fname,                 /* this is where the name will go */
			sizeof(fname),                   /* BufferLength -- too small! */
			&name_length,
			0);
		return std::string(fname);
	}

	virtual int getFieldLength(int index) const
	{
		SQLLEN length = 0;
		SQLRETURN retcode;
		retcode = SQLColAttribute(
			res_,
			index + 1,                    /* the Column number */
			SQL_DESC_LENGTH,        /* the field identifier, = 1011 */
			NULL,                 /* this is where the name will go */
			NULL,                   /* BufferLength -- too small! */
			NULL,
			&length);
		return length;
		//return PQgetlength(res_, pos_, index);
	}

	virtual int getFieldLength(const char* name) const
	{

		throw mapnik::datasource_exception("ResultSet getFieldLength not implemented");
		/*int col = PQfnumber(res_, name);
		if (col >= 0)
		{
		return PQgetlength(res_, pos_, col);
		}*/
		return 0;
	}

	virtual int getTypeOID(int index) const
	{
		SQLINTEGER dataType;
		SQLColAttribute(res_, index + 1, SQL_DESC_TYPE, NULL, 0, NULL, &dataType);
		return dataType;
		//return PQftype(res_, index);
	}

	virtual int getTypeOID(const char* name) const
	{
		throw mapnik::datasource_exception("ResultSet getTypeOID(const char* name) not implemented");
		return 0;
		/* int col = PQfnumber(res_, name);
		if (col >= 0)
		{
		return PQftype(res_, col);
		}
		return 0;*/
	}

	virtual bool isNull(int index) const
	{
		SQLLEN length;
		SQLRETURN retcode;
		//unsigned char *value = new unsigned char[1];
		unsigned char value[1];

		retcode = SQLGetData(res_, index + 1, SQL_C_BINARY, value, 0, &length);

		return static_cast<bool>(length == SQL_NULL_DATA);
	}


	virtual const int getInt(int index) const
	{
		SQLINTEGER intvalue;
		SQLGetData(res_, index + 1, SQL_C_SLONG, &intvalue, 0, NULL);
		return intvalue;
		// return PQgetvalue(res_, pos_, index);
	}
	virtual const double getDouble(int index) const
	{
		double value;
		SQLGetData(res_, index + 1, SQL_C_DOUBLE, &value, 0, NULL);
		return value;
		// return PQgetvalue(res_, pos_, index);
	}
	virtual const float getFloat(int index) const
	{
		float value;
		SQLGetData(res_, index + 1, SQL_C_FLOAT, &value, 0, NULL);
		return value;
		// return PQgetvalue(res_, pos_, index);
	}
	virtual const std::string getString(int index) const
	{
		SQLLEN length = 0;
		SQLRETURN retcode;

#ifdef _WINDOWS
		auto str = std::wstring();
		wchar_t buffer[255];
#else
		auto str = std::string();
		char buffer[255];
#endif			

		retcode = SQLGetData(res_, index + 1, SQL_C_WCHAR, &buffer, sizeof(buffer), &length);

		if (retcode == SQL_SUCCESS && length != SQL_NULL_DATA){			
#ifdef _WINDOWS
			return mapnik::utf16_to_utf8(buffer);			
#else
			return buffer;
#endif	
		}else if (retcode == SQL_SUCCESS_WITH_INFO && length != SQL_NULL_DATA){
			
			str.reserve(length);
			str.append(buffer);
			
			while ((retcode = SQLGetData(res_, index + 1, SQL_C_WCHAR, &buffer, sizeof(buffer),
					&length)) != SQL_NO_DATA) {			
				str.append(buffer);
			}
		}
		
#ifdef _WINDOWS
		return mapnik::utf16_to_utf8(str);
#else
		return str;
#endif	

		// return PQgetvalue(res_, pos_, index);
	}
	virtual const std::vector<const char> getBinary(int index) const
	{
		SQLLEN length = 0;
		SQLRETURN retcode;
		std::string value;
		char bit[1];

		retcode = SQLGetData(res_, index + 1, SQL_C_BINARY, bit, 0, &length);
		if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO){
			if (length != SQL_NULL_DATA){
				// char* binvalue = new char[length*2];
								
				std::vector<const char> binvalue(length, 0);
				retcode = SQLGetData(res_, index + 1, SQL_C_BINARY, &binvalue[0], length, &length);

				//char* binvalue = new char[80000];
				//retcode = SQLGetData(res_, index + 1, SQL_C_BINARY, binvalue, length, &length);


				return binvalue;
				// return std::vector<const char>(binvalue, binvalue + sizeof binvalue / sizeof binvalue[0]);
			}
		}
		return std::vector<const char>();

		// return PQgetvalue(res_, pos_, index);
	}
	virtual const char* getValue4(int index) const
	{
		throw mapnik::datasource_exception("ResultSet getValue(intindex) not implemented");
		return "novalue";
		// return PQgetvalue(res_, pos_, index);
	}

	virtual const char* getValue5(const char* name) const
	{
		std::string a = "ResultSet getValue([const char* name]";
		a += ((char*)name);
		a += ") not implemented";
		throw mapnik::datasource_exception(a.c_str());
		return "novalue";
		/*
		int col = PQfnumber(res_, name);
		if (col >= 0)
		{
		return getValue(col);
		}
		return 0;*/
	}

private:
	SQLHANDLE res_;
	int pos_;
	int numTuples_;
};

#endif // MSSQL_RESULTSET_HPP
