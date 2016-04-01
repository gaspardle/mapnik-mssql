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

#ifndef MSSQL_CONNECTION_HPP
#define MSSQL_CONNECTION_HPP

// mapnik
#include <mapnik/debug.hpp>
#include <mapnik/datasource.hpp>
#include <mapnik/timer.hpp>

// std
#include <memory>
#include <sstream>
#include <iostream>

#include <sql.h>
#include <sqlext.h>
#include <sqlucode.h>
#include "odbc.hpp"

#include "resultset.hpp"


class Connection
{
public:
    Connection(SQLHANDLE sqlenvhandle, std::string const& connection_str, boost::optional<std::string> const& password)
        :
        closed_(false),
        pending_(false)
    {
        std::string connect_with_pass = connection_str;
        if (password && !password->empty())
        {
            connect_with_pass += " PWD=" + *password;
        }
  
        if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_DBC, sqlenvhandle, &sqlconnectionhandle))
        {
            close();
            throw mapnik::datasource_exception("Mssql Plugin: SQLAllocHandle SQL_HANDLE_DBC failed");
        }

        SQLCHAR  retconstring[1024];
        SQLSMALLINT OutConnStrLen;
        SQLRETURN retcode = SQLDriverConnectA(sqlconnectionhandle,
                                              NULL,
                                              (SQLCHAR*)connect_with_pass.c_str(),
                                              SQL_NTS,
                                              retconstring,
                                              1024,
                                              &OutConnStrLen,//NULL,
                                              SQL_DRIVER_NOPROMPT);
        if (!(retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO))
        {
            std::string err_msg = "Mssql Plugin: ";
            err_msg += getOdbcError(SQL_HANDLE_DBC, sqlconnectionhandle);
            err_msg += "\nSQLDriverConnect Error, Connection string: '";
            err_msg += connection_str;
            err_msg += "'\n";
            close();
            throw mapnik::datasource_exception(err_msg);
        }

    }

    ~Connection()
    {
        if (!closed_)
        {

            SQLDisconnect(sqlconnectionhandle);
            SQLFreeHandle(SQL_HANDLE_DBC, sqlconnectionhandle);

            MAPNIK_LOG_DEBUG(mssql) << "mssql_connection: sql server connection closed";

            closed_ = true;
        }

    }

    bool execute(std::string const& sql)
    {
#ifdef MAPNIK_STATS
        mapnik::progress_timer __stats__(std::clog, std::string("mssql_connection::execute ") + sql);
#endif
        SQLHANDLE hstmt = NULL;
        SQLRETURN retcode;

        if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_STMT, sqlconnectionhandle, &hstmt))
        {
            throw mapnik::datasource_exception("SQLAllocHandle error");
        }

        retcode = SQLExecDirectA(hstmt, (SQLCHAR*)sql.c_str(), SQL_NTS);				
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

        return retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO;
    }

    std::shared_ptr<ResultSet> executeQuery(std::string const& sql)
    {
#ifdef MAPNIK_STATS
        mapnik::progress_timer __stats__(std::clog, std::string("mssql_connection::execute_query ") + sql);
#endif
        debug_current_sql = sql;
        SQLHANDLE hstmt = NULL;
        SQLRETURN retcode;

        if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_STMT, sqlconnectionhandle, &hstmt))
        {
            throw mapnik::datasource_exception("SQLAllocHandle error");
        }
        retcode = SQLExecDirectA(hstmt, (SQLCHAR*)sql.c_str(), SQL_NTS);

        if (!(retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO))
        {
            std::string err_msg = getOdbcError(SQL_HANDLE_STMT, hstmt);
            err_msg += "\nFull sql was: '";
            err_msg += sql;
            err_msg += "'\n";

            SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
            throw mapnik::datasource_exception(err_msg);
        }

        return std::make_shared<ResultSet>(hstmt);
    }



    bool executeAsyncQuery(std::string const& sql)
    {
        debug_current_sql = sql;
        SQLRETURN retcode;

        if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_STMT, sqlconnectionhandle, &async_hstmt))
        {
            throw mapnik::datasource_exception("cant SQLAllocHandle");
        }

#ifdef _WIN32
        //freetds does not seem to support async
        SQLSetStmtAttr(async_hstmt, SQL_ATTR_ASYNC_ENABLE, (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0);
#endif

        retcode = SQLExecDirectA(async_hstmt, (SQLCHAR*)sql.c_str(), SQL_NTS);

        if (!(retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO || retcode == SQL_STILL_EXECUTING))
        {
            std::string err_msg = "Mssql Plugin: ";
            err_msg += getOdbcError(SQL_HANDLE_DBC, sqlconnectionhandle);
            err_msg += "\nin executeAsyncQuery Full sql was: '";
            err_msg += sql;
            err_msg += "'\n";
            clearAsyncResult(async_hstmt);
            close();
            throw mapnik::datasource_exception(err_msg);
        }
        pending_ = true;
        return 1;
    }

    SQLRETURN getResult()
    {
#ifndef _WIN32
        //freetds does not seem to support async
        return SQL_SUCCESS;
#endif
        SQLRETURN retcode;

        while (true)
        {

            retcode = SQLExecDirectA(async_hstmt, (SQLCHAR*)"", SQL_NTS);

            if (retcode != SQL_STILL_EXECUTING)
            {
                break;
            }
        }

        SQLSetStmtAttr(async_hstmt, SQL_ATTR_ASYNC_ENABLE, (SQLPOINTER)SQL_ASYNC_ENABLE_OFF, 0);
        return retcode;
    }

    std::shared_ptr<ResultSet> getNextAsyncResult()
    {
        SQLRETURN result = getResult();
        if (!(result == SQL_SUCCESS || result == SQL_SUCCESS_WITH_INFO))
        {
            std::string err_msg = "Mssql Plugin: ";
            std::string err_status = getOdbcError(SQL_HANDLE_STMT, async_hstmt);
            err_msg += err_status + "\n in getNextAsyncResult";
            clearAsyncResult(async_hstmt);
            // We need to guarde against losing the connection
            // (i.e db restart) so here we invalidate the full connection
            close();
            throw mapnik::datasource_exception(err_msg);
        }
        return std::make_shared<ResultSet>(async_hstmt);
    }


    std::shared_ptr<ResultSet> getAsyncResult()
    {

        SQLRETURN result = getResult();
        if (result == SQL_INVALID_HANDLE)
        {
            throw mapnik::datasource_exception("Mssql Plugin: invalid handle in getAsyncResult");
        }
        if (!(result == SQL_SUCCESS || result == SQL_SUCCESS_WITH_INFO))
        {

            std::string err_msg = "Mssql Plugin: ";
            std::string err_status = getOdbcError(SQL_HANDLE_STMT, async_hstmt);
            err_msg += err_status + "\n in getAsyncResult";
            err_msg += err_msg + "\n query: " + debug_current_sql;
            clearAsyncResult(async_hstmt);
            // We need to be guarded against losing the connection
            // (i.e db restart), we invalidate the full connection
            close();
            throw mapnik::datasource_exception(err_msg);
        }
        return std::make_shared<ResultSet>(async_hstmt);
    }

    bool isOK() const
    {
        if (closed_)
        {
            return false;
        }
        return true;

        SQLINTEGER dead;

        //SQLRETURN retcode = SQLGetConnectAttr(sqlconnectionhandle, SQL_COPT_SS_CONNECTION_DEAD, &dead, SQL_IS_UINTEGER, NULL);
        SQLRETURN retcode = SQLGetConnectAttr(sqlconnectionhandle, SQL_ATTR_CONNECTION_DEAD, &dead, 0, NULL);
        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            return (!closed_) && (dead != SQL_CD_TRUE);
        }
        else
        {
            return false;
        }
    }

    bool isPending() const
    {
        return pending_;
    }

    void close()
    {
        if (!closed_)
        {
            SQLDisconnect(sqlconnectionhandle);
            SQLFreeHandle(SQL_HANDLE_DBC, sqlconnectionhandle);
            //SQLFreeHandle(SQL_HANDLE_ENV, sqlenvhandle);

            MAPNIK_LOG_DEBUG(mssql) << "mssql_connection: datasource closed, also closing connection";

            closed_ = true;
        }
    }


private:
    //SQLHANDLE sqlenvhandle;
    SQLHANDLE sqlconnectionhandle;
    SQLHANDLE async_hstmt;

    bool closed_;
    bool pending_;

    std::string debug_current_sql;
   
    void clearAsyncResult(SQLHANDLE hstmt)
    {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        pending_ = false;
    }
};



#endif //CONNECTION_HPP
