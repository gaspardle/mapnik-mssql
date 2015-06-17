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

#ifndef MSSQL_CONNECTIONFTDS_HPP
#define MSSQL_CONNECTIONFTDS_HPP

// mapnik
#include <mapnik/debug.hpp>
#include <mapnik/datasource.hpp>
#include <mapnik/timer.hpp>

// std
#include <memory>
#include <sstream>
#include <iostream>

#include "dblib_resultset.hpp"


int
msg_handler(DBPROCESS *dbproc, DBINT msgno, int msgstate, int severity,
    char *msgtext, char *srvname, char *procname, int line)
{
    enum { changed_database = 5701, changed_language = 5703 };

    if (msgno == changed_database || msgno == changed_language)
        return 0;
 
    if (msgno > 0) {
        fprintf(stderr, "Msg %ld, Level %d, State %d\n",
            (long)msgno, severity, msgstate);

        if (strlen(srvname) > 0)
            fprintf(stderr, "Server '%s', ", srvname);
        if (strlen(procname) > 0)
            fprintf(stderr, "Procedure '%s', ", procname);
        if (line > 0)
            fprintf(stderr, "Line %d", line);

        fprintf(stderr, "\n\t");
    }
    fprintf(stderr, "%s\n", msgtext);

    if (severity > 10) {
        fprintf(stderr, "%s: error: severity %d > 10, exiting\n",
            "mssql", severity);
        //throw mapnik::datasource_exception("error: severity");
        exit(severity);
    }

    return 0;
}

int
err_handler(DBPROCESS * dbproc, int severity, int dberr, int oserr,
    char *dberrstr, char *oserrstr)
{
    if (dberr) {
        fprintf(stderr, "%s: Msg %d, Level %d\n",
            "mssql", dberr, severity);
        fprintf(stderr, "%s\n\n", dberrstr);
    }
    else {
        fprintf(stderr, "%s: DB-LIBRARY error:\n\t", "mssql");
        fprintf(stderr, "%s\n", dberrstr);
    }

    return INT_CANCEL;
}

class ConnectionDblib : public IConnection
{
public:



    ConnectionDblib(std::string const& servername, std::string const& username, std::string const& password, boost::optional<std::string> const& dbname)
        :
        closed_(false),
        pending_(false)
    {
        

        LOGINREC *login;
       
        dblib::RETCODE erc;

        if (dbinit() == FAIL) {
            throw mapnik::datasource_exception("Mssql Plugin: dbinit()");            
        }

        
        dberrhandle(err_handler);
        dbmsghandle(msg_handler);

        
        if ((login = dblogin()) == NULL) {
            throw mapnik::datasource_exception("Mssql Plugin: unable to allocate login structure");            
        }
        
        dbsetlversion(login, DBVERSION_73);
       
        
        if(username != nullptr && password != nullptr) {
            DBSETLUSER(login, username.c_str());
            DBSETLPWD(login, password.c_str());
            DBSETLCHARSET(login, "UTF-8");
        }
      
        if ((dbproc = dbopen(login, servername.c_str())) == NULL) {
            throw mapnik::datasource_exception("Mssql Plugin: unable to connect to " + servername + " as " + username.c_str());
         
        }

        if (dbname && !dbname->empty() && (erc = dbuse(dbproc, dbname.get().c_str())) == FAIL)
        {
            throw mapnik::datasource_exception("Mssql Plugin: unable to use to database " + *dbname);           
        }   

        //set defaults
        erc = dbsetopt(dbproc, DBQUOTEDIDENT, "ON", -1);
        if(erc == FAIL) {
            throw mapnik::datasource_exception("Mssql Plugin: DBQUOTEDIDENT fail ");
        }       
        
    
       /* if ((erc = dbcmd(dbproc, "set quoted_identifier on; set ansi_padding on; set ansi_nulls on;")) == FAIL) {
            throw mapnik::datasource_exception("dbcmd() failed");
        }

        if ((erc = dbsqlexec(dbproc)) == FAIL) {
            throw mapnik::datasource_exception("dbsqlexec() failed");
        }*/

    }

    ~ConnectionDblib()
    {
        if (!closed_)
        {
            dbclose(dbproc);           
            MAPNIK_LOG_DEBUG(mssql) << "mssql_connection: sql server connection closed";

            closed_ = true;
        }

    }

    bool execute(std::string const& sql)
    {
#ifdef MAPNIK_STATS
        mapnik::progress_timer __stats__(std::clog, std::string("mssql_connection::execute ") + sql);
#endif
        throw mapnik::datasource_exception("not implemented");

    }

    std::shared_ptr<IResultSet> executeQuery(std::string const& sql)
    {
#ifdef MAPNIK_STATS
        mapnik::progress_timer __stats__(std::clog, std::string("mssql_connection::execute_query ") + sql);
#endif
        debug_current_sql = sql;
        
        dblib::RETCODE retcode;
                
       // std::cout << "connection executeQuery: " << std::endl << sql.c_str() << std::endl << std::endl;
      
        if ((retcode = dbcmd(dbproc, sql.c_str())) == FAIL) {
            throw mapnik::datasource_exception("dbcmd() failed");          
        }

        if ((retcode = dbsqlexec(dbproc)) == FAIL) {
            throw mapnik::datasource_exception("dbsqlexec() failed");          
        }
          

        return std::make_shared<DBLibResultSet>(dbproc);
    }



    bool executeAsyncQuery(std::string const& sql)
    {
        throw mapnik::datasource_exception("executeAsyncQuery not implemented");
        /*debug_current_sql = sql;
        SQLRETURN retcode;

        if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_STMT, sqlconnectionhandle, &async_hstmt)) {
            throw mapnik::datasource_exception("cant SQLAllocHandle");
        }

#ifdef _WIN32
        //freetds does not seem to support async
        SQLSetStmtAttr(async_hstmt, SQL_ATTR_ASYNC_ENABLE, (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0);
#endif

        std::cout << "connection executeAsyncQuery: " << std::endl << sql.c_str() << std::endl << std::endl;
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
        return 1;*/
    }

    SQLRETURN getResult()
    {
        throw mapnik::datasource_exception("getResult not implemented");
        /*
#ifndef _WIN32
        //freetds does not seem to support async
        return SQL_SUCCESS;
#endif
        SQLRETURN retcode;

        while (true) {

            retcode = SQLExecDirectA(async_hstmt, (SQLCHAR*)"", SQL_NTS);

            if (retcode != SQL_STILL_EXECUTING)
                break;
        }

        SQLSetStmtAttr(async_hstmt, SQL_ATTR_ASYNC_ENABLE, (SQLPOINTER)SQL_ASYNC_ENABLE_OFF, 0);
        return retcode;*/
    }

    std::shared_ptr<IResultSet> getNextAsyncResult()
    {  
        throw mapnik::datasource_exception("getNextAsyncResult not implemented");
       /* SQLRETURN result = getResult();
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
        return std::make_shared<ResultSet>(async_hstmt);*/
    }


    std::shared_ptr<IResultSet> getAsyncResult()
    {
        throw mapnik::datasource_exception("getAsyncResult not implemented");
       /* SQLRETURN result = getResult();
        if (result == SQL_INVALID_HANDLE) {
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
        return std::make_shared<ResultSet>(async_hstmt);*/
    }

    bool isOK() const
    {
        if (closed_) {
            return false;
        }
        return true;        
    }

    bool isPending() const
    {
        return pending_;
    }

    void close()
    {
        if (!closed_)
        {
            dbclose(dbproc);
            MAPNIK_LOG_DEBUG(mssql) << "mssql_connection: datasource closed, also closing connection";

            closed_ = true;
        }
    }


private:

    DBPROCESS *dbproc;

    bool closed_;
    bool pending_;

    std::string debug_current_sql;
};



#endif //MSSQL_CONNECTIONFTDS_HPP
