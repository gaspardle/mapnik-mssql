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
#include <mapnik/utils.hpp>
// boost
#include <boost/make_shared.hpp>

// std
#include <sstream>
#include <iostream>



#include <sql.h>
#include <sqlext.h>

#include "resultset.hpp"


class Connection
{
public:
    Connection(std::string const& connection_str,boost::optional<std::string> const& password)
        : cursorId(0),
          closed_(false)
    {
        std::string connect_with_pass = connection_str;
        /*if (password && !password->empty())
        {
            connect_with_pass += " password=" + *password;
        }*/

        if(SQL_SUCCESS!=SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sqlenvhandle))
            throw mapnik::datasource_exception("Mssql Plugin: SQLAllocHandle");// goto FINISHED;

        if(SQL_SUCCESS!=SQLSetEnvAttr(sqlenvhandle,SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0)) 
            throw mapnik::datasource_exception("Mssql Plugin: SQLSetEnvAttr");// goto FINISHED;
    
        if(SQL_SUCCESS!=SQLAllocHandle(SQL_HANDLE_DBC, sqlenvhandle, &sqlconnectionhandle))
            throw mapnik::datasource_exception("Mssql Plugin: SQLAllocHandle");// goto FINISHED;

        SQLCHAR  retconstring[1024];
    	SQLSMALLINT OutConnStrLen;
        SQLRETURN retcode  = SQLDriverConnect (sqlconnectionhandle, 
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
            err_msg += status(SQL_HANDLE_DBC, sqlconnectionhandle);
            err_msg += "\nConnection string: '";
            err_msg += connection_str;
            err_msg += "'\n";
            throw mapnik::datasource_exception(err_msg);
        }

        /*conn_ = PQconnectdb(connect_with_pass.c_str());
        if (PQstatus(conn_) != CONNECTION_OK)
        {
            std::string err_msg = "Mssql Plugin: ";
            err_msg += status();
            err_msg += "\nConnection string: '";
            err_msg += connection_str;
            err_msg += "'\n";
            throw mapnik::datasource_exception(err_msg);
        }*/
         
    }

    ~Connection()
    {
        if (! closed_)
        {
           
            SQLDisconnect(sqlconnectionhandle);
            SQLFreeHandle(SQL_HANDLE_DBC, sqlconnectionhandle);
            SQLFreeHandle(SQL_HANDLE_ENV, sqlenvhandle);
           

            MAPNIK_LOG_DEBUG(mssql) << "mssql_connection: sql server connection closed";

            closed_ = true;
        }
    }

    bool execute(std::string const& sql) const
    {
#ifdef MAPNIK_STATS
        mapnik::progress_timer __stats__(std::clog, std::string("mssql_connection::execute ") + sql);
#endif
        throw mapnik::datasource_exception("execute!");
        //PGresult *result = PQexec(conn_, sql.c_str());
        //bool ok = (result && (PQresultStatus(result) == PGRES_COMMAND_OK));
        //PQclear(result);
        //return ok;
    }

    boost::shared_ptr<ResultSet> executeQuery(std::string const& sql, int type = 0) const
    {
#ifdef MAPNIK_STATS
        mapnik::progress_timer __stats__(std::clog, std::string("mssql_connection::execute_query ") + sql);
#endif
        
        SQLHANDLE hstmt = NULL;
        SQLRETURN retcode;
        if(SQL_SUCCESS!=SQLAllocHandle(SQL_HANDLE_STMT, sqlconnectionhandle, &hstmt))
            throw mapnik::datasource_exception("cant SQLAllocHandle");
        
        //PGresult* result = 0;
        if (type == 1)
        {
            //binary
            //result = PQexecParams(conn_,sql.c_str(), 0, 0, 0, 0, 0, 1);
            retcode = SQLExecDirect(hstmt, (SQLCHAR*)sql.c_str(), SQL_NTS);
        }
        else
        {
           //text
           // result = PQexec(conn_, sql.c_str());
            retcode = SQLExecDirect(hstmt, (SQLCHAR*)sql.c_str(), SQL_NTS);
        }

          
        if (!( retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO))
       // if (! result || (PQresultStatus(result) != PGRES_TUPLES_OK))
        {
            std::string err_msg = status( SQL_HANDLE_STMT,hstmt);
            err_msg += "\nFull sql was: '";
            err_msg += sql;
            err_msg += "'\n";
           
            SQLFreeHandle(SQL_HANDLE_STMT, hstmt );
            // PQclear(result);
            

            throw mapnik::datasource_exception(err_msg);
        }

        return boost::make_shared<ResultSet>(hstmt);
    }

     std::string status() const
    {
        return status(SQL_HANDLE_DBC, sqlconnectionhandle);
    }
    std::string status(unsigned int handletype, const SQLHANDLE& handle) const
    {
        
        std::string  status = "{Status}";
  
        SQLCHAR  sqlstate[6];
        SQLCHAR  message[SQL_MAX_MESSAGE_LENGTH];
	    SQLINTEGER  NativeError;
	    SQLSMALLINT   i, MsgLen;
	    SQLRETURN     rc2;
        //mapnik::transcoder tr("wcs");
        

	        // Get the status records.
        i = 1;
        while ((rc2 = SQLGetDiagRec(handletype, handle, i, sqlstate, &NativeError,           
            message, sizeof(message), &MsgLen)) != SQL_NO_DATA) {
            status  += "("  + std::to_string((LONGLONG)i) +")";
            status += "\nSQLState: ";
            status +=((char*)&sqlstate[0]); 
            status += "\nNativeError: " + std::to_string((LONGLONG)NativeError);
            status += "\nMessage: ";
            status +=(char*)&message[0];                    
            status += "\nMsgLen: " + std::to_string((LONGLONG)MsgLen);
          
            i++;
        }

        /*if (conn_)
        {
            status = PQerrorMessage(conn_);
        }
        else
        {
            status = "Uninitialized connection";
        }*/
        
        status += "{status end}";
        
       
        return status;
    }

    std::string client_encoding() const
    {
       
        return "UTF8";
        //return PQparameterStatus(conn_, "client_encoding");
    }

    bool isOK() const
    {
        
        SQLINTEGER dead;
        //SQL_COPT_SS_CONNECTION_DEAD
        SQLGetConnectAttr(sqlconnectionhandle, SQL_ATTR_CONNECTION_DEAD, &dead, 0, NULL);
        return (!closed_) && (dead != SQL_CD_TRUE);
        //return (!closed_) && (PQstatus(conn_) != CONNECTION_BAD);
    }

    void close()
    {
        if (! closed_)
        {            
            MAPNIK_LOG_DEBUG(mssql) << "mssql_connection: datasource closed, also closing connection";

            closed_ = true;
        }
    }

    std::string new_cursor_name()
    {
        std::ostringstream s;
        s << "mapnik_" << (cursorId++);
        return s.str();
    }

private:
    SQLHANDLE sqlenvhandle;    
    SQLHANDLE sqlconnectionhandle;
   
    int cursorId;
    bool closed_;
};

#endif //CONNECTION_HPP
