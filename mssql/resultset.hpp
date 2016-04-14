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

#include "mapnik/util/utf_conv_win.hpp"

#include "odbc.hpp"
#include <mapnik/datasource.hpp>
#include <mapnik/debug.hpp>
#include <sql.h>
///#include <sqlext.h>

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
    virtual const boost::optional<int> getInt(int index) const = 0;
    virtual const boost::optional<long long> getBigInt(int index) const = 0;
    virtual const boost::optional<double> getDouble(int index) const = 0;
    virtual const boost::optional<float> getFloat(int index) const = 0;
    virtual const std::string getString(int index) const = 0;
    virtual const std::wstring getWString(int index) const = 0;
    virtual const std::vector<char> getBinary(int index) const = 0;
};

class ResultSet : public IResultSet, private mapnik::util::noncopyable
{
  public:
    ResultSet(SQLHANDLE res)
        : res_(res),
          is_closed_(false)
    {
    }

    virtual void close()
    {
        if (!is_closed_)
        {
            SQLFreeHandle(SQL_HANDLE_STMT, res_);
            is_closed_ = true;
        }
    }

    virtual ~ResultSet()
    {
        close();
    }

    virtual int getNumFields() const
    {
        SQLSMALLINT column_count;

        if (SQLNumResultCols(res_, &column_count) < 0)
        {
            throw mapnik::datasource_exception("resultset getNumFields error");
            //return 0;
        }
        return column_count;
    }

    virtual bool next()
    {

        SQLRETURN retcode;
        retcode = SQLFetch(res_);

        if (retcode == SQL_STILL_EXECUTING)
        {
            while (retcode == SQL_STILL_EXECUTING)
            {
                retcode = SQLFetch(res_);
            }
        }

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            return true;
        }
        else if (retcode == SQL_NO_DATA)
        {
            return false;
        }
        else
        {
            std::string errormsg = getOdbcError(SQL_HANDLE_STMT, res_);
            throw mapnik::datasource_exception("resultset next error: " + errormsg);
        }
    }

    virtual const std::string getFieldName(int index) const
    {
        char fname[256];
        SQLSMALLINT name_length;
        SQLRETURN retcode;

        retcode = SQLColAttributeA(
            res_,
            index + 1,
            SQL_DESC_NAME,
            fname,
            sizeof(fname),
            &name_length,
            0);

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            return std::string(fname);
        }
        else
        {
            throw mapnik::datasource_exception("Error in Resultset getFieldName: " + getOdbcError(SQL_HANDLE_STMT, res_));
        }
        return 0;
    }

    virtual int getFieldLength(int index) const
    {
        SQLLEN length = 0;
        SQLRETURN retcode;
        retcode = SQLColAttribute(
            res_,
            index + 1,
            SQL_DESC_LENGTH,
            NULL,
            NULL,
            NULL,
            &length);

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            return (int)length;
        }

        return 0;
    }

    virtual int getFieldLength(const char* name) const
    {
        throw mapnik::datasource_exception("ResultSet getFieldLength not implemented");
        return 0;
    }

    virtual int getTypeOID(int index) const
    {
        SQLLEN dataType = 0;
        SQLRETURN retcode;

        retcode = SQLColAttribute(res_, index + 1, SQL_DESC_TYPE, NULL, 0, NULL, &dataType);

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            return (int)dataType;
        }
        else
        {
            std::string errormsg = getOdbcError(SQL_HANDLE_STMT, res_);
            MAPNIK_LOG_ERROR(mssql) << "getTypeOID error: " << errormsg;
        }

        return 0;
    }

    virtual int getTypeOID(const char* name) const
    {
        throw mapnik::datasource_exception("ResultSet getTypeOID(const char* name) not implemented");
        return 0;
    }

    virtual bool isNull(int index) const
    {
        SQLLEN length;
        SQLRETURN retcode;

        unsigned char value[1];

        retcode = SQLGetData(res_, index + 1, SQL_C_BINARY, value, 0, &length);

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            return static_cast<bool>(length == SQL_NULL_DATA);
        }

        return 0;
    }

    virtual const boost::optional<long long> getBigInt(int index) const
    {
        SQLBIGINT intvalue;
        SQLRETURN retcode;
        SQLLEN ind;
        retcode = SQLGetData(res_, index + 1, SQL_C_SBIGINT, &intvalue, 0, &ind);

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            if (ind == SQL_NULL_DATA)
            {
                return boost::optional<long long>();
            }
            return intvalue;
        }
        else
        {
            std::string errormsg = getOdbcError(SQL_HANDLE_STMT, res_);
            MAPNIK_LOG_ERROR(mssql) << "getBigInt error: " << errormsg;
        }
        return 0;
    }

    virtual const boost::optional<int> getInt(int index) const
    {
        SQLINTEGER intvalue;
        SQLRETURN retcode;
        SQLLEN ind;
        retcode = SQLGetData(res_, index + 1, SQL_C_SLONG, &intvalue, 0, &ind);

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            if (ind == SQL_NULL_DATA)
            {
                return boost::optional<int>();
            }
            return intvalue;
        }
        else
        {
            std::string errormsg = getOdbcError(SQL_HANDLE_STMT, res_);
            MAPNIK_LOG_ERROR(mssql) << "getInt error: " << errormsg;
        }
        return 0;
    }

    virtual const boost::optional<double> getDouble(int index) const
    {
        double value;
        SQLRETURN retcode;
        SQLLEN ind;
        retcode = SQLGetData(res_, index + 1, SQL_C_DOUBLE, &value, 0, &ind);

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            if (ind == SQL_NULL_DATA)
            {
                return boost::optional<double>();
            }
            return value;
        }
        else
        {
            std::string errormsg = getOdbcError(SQL_HANDLE_STMT, res_);
            MAPNIK_LOG_ERROR(mssql) << "getDouble error: " << errormsg;
        }
        return 0;
    }

    virtual const boost::optional<float> getFloat(int index) const
    {
        float value;
        SQLRETURN retcode;
        SQLLEN ind;
        retcode = SQLGetData(res_, index + 1, SQL_C_FLOAT, &value, 0, &ind);

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            if (ind == SQL_NULL_DATA)
            {
                return boost::optional<float>();
            }
            return value;
        }
        else
        {
            std::string errormsg = getOdbcError(SQL_HANDLE_STMT, res_);
            MAPNIK_LOG_ERROR(mssql) << "getFloat error: " << errormsg;
        }

        return 0;
    }

    virtual const std::wstring getWString(int index) const
    {
        SQLLEN length = 0;
        SQLRETURN retcode;

        std::wstring str;
        wchar_t buffer[255];

        retcode = SQLGetData(res_, index + 1, SQL_C_WCHAR, &buffer, sizeof(buffer), &length);

        if (retcode == SQL_SUCCESS && length != SQL_NULL_DATA)
        {

            return buffer;
        }
        else if (retcode == SQL_SUCCESS_WITH_INFO && length != SQL_NULL_DATA)
        {

            str.reserve(length);
            str.append(buffer);

            while ((retcode = SQLGetData(res_, index + 1, SQL_C_WCHAR, &buffer, sizeof(buffer),
                                         &length)) != SQL_NO_DATA)
            {
                str.append(buffer);
            }
        }

        return str;
    }

    virtual const std::string getString(int index) const
    {

#ifdef _WINDOWS
        std::string s = mapnik::utf16_to_utf8(getWString(index));
        return s;
#endif
        SQLLEN length = 0;
        SQLRETURN retcode;

        std::string str;
        char buffer[255];

        retcode = SQLGetData(res_, index + 1, SQL_C_CHAR, &buffer, sizeof(buffer), &length);

        if (retcode == SQL_SUCCESS && length != SQL_NULL_DATA)
        {

            return buffer;
        }
        else if (retcode == SQL_SUCCESS_WITH_INFO && length != SQL_NULL_DATA)
        {

            str.reserve(length);
            str.append(buffer);

            while ((retcode = SQLGetData(res_, index + 1, SQL_C_CHAR, &buffer, sizeof(buffer),
                                         &length)) != SQL_NO_DATA)
            {
                str.append(buffer);
            }
        }
        else
        {
            std::string errormsg = getOdbcError(SQL_HANDLE_STMT, res_);
            MAPNIK_LOG_ERROR(mssql) << "getString error: " << errormsg;
        }

        return str;
    }

    virtual const std::vector<char> getBinary(int index) const
    {
        SQLLEN length = 0;
        SQLRETURN retcode;
        std::string value;
        char bit[1];

        retcode = SQLGetData(res_, index + 1, SQL_C_BINARY, bit, 0, &length);
        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            if (length != SQL_NULL_DATA)
            {

                std::vector<char> binvalue(length, 0);
                retcode = SQLGetData(res_, index + 1, SQL_C_BINARY, (SQLPOINTER)&binvalue[0], length, &length);
                if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
                {
                    return binvalue;
                }
            }
        }
        else
        {
            std::string errormsg = getOdbcError(SQL_HANDLE_STMT, res_);
            MAPNIK_LOG_ERROR(mssql) << "getBinary error: " << errormsg;
        }

        return std::vector<char>();
    }

  private:
    SQLHANDLE res_;
    bool is_closed_;
};

#endif // MSSQL_RESULTSET_HPP
