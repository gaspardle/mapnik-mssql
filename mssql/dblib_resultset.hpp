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

#ifndef MSSQL_DBLIBRESULTSET_HPP
#define MSSQL_DBLIBRESULTSET_HPP

namespace dblib{ //XXX because odbc and freetds conflict on RETCODE
    #include <sybfront.h>
    #include <sybdb.h>
}

#include <mapnik/datasource.hpp>
#include "iresultset.hpp"

using namespace dblib;

class DBLibResultSet : public IResultSet, private mapnik::util::noncopyable
{

public:
    DBLibResultSet(DBPROCESS* res)
        : dbproc_(res),
        is_closed_(false)
    {
    
        dblib::RETCODE retcode = dbresults(dbproc_);
    }

    virtual void close()
    {
        if (!is_closed_)
        {         
            is_closed_ = true;
        }
    }

    virtual ~DBLibResultSet()
    {
        close();
    }

    virtual int getNumFields() const
    {
       return dbnumcols(dbproc_); 
    }
    
    virtual bool next()
    {

        int row_code = dbnextrow(dbproc_);

        switch (row_code) {
            case NO_MORE_ROWS:
                return false;
            case REG_ROW:           
                break;

            case BUF_FULL:
                assert(row_code != BUF_FULL);
                break;

            case FAIL:
                throw mapnik::datasource_exception("dbresults failed");           
                break;

           				
               // printf("Data for computeid %d ignored\n", row_code);
        }

        return true;

    }

    virtual const std::string getFieldName(int index) const
    {
        char* colname = dbcolname(dbproc_, index + 1);
        return std::string(colname);
       
    }

    virtual int getFieldLength(int index) const
    {
        return dbdatlen(dbproc_, index + 1);
    }

    virtual int getFieldLength(const char* name) const
    {

        throw mapnik::datasource_exception("ResultSet getFieldLength not implemented");
        return 0;
    }

    virtual int getTypeOID(int index) const
    {
        return dbcoltype(dbproc_, index + 1);       
    }

    virtual int getTypeOID(const char* name) const
    {
        throw mapnik::datasource_exception("ResultSet getTypeOID(const char* name) not implemented");
        return 0;
    }

    virtual bool isNull(int index) const
    {
        return dbdatlen(dbproc_, index + 1) == 0;        
    }


    virtual const int getInt(int index) const
    {
        auto coltype = dbcoltype(dbproc_, index + 1);       

        long intvalue;
        dbconvert(dbproc_, coltype,
            (dbdata(dbproc_, index + 1)), (DBINT)-1, SYBINT4,
            (byte*)&intvalue, (DBINT)-1);


        //long intvalue = *((DBINT *)dbdata(dbproc_, index + 1));
        return intvalue;

    }
    virtual const double getDouble(int index) const
    { 
        auto coltype = dbcoltype(dbproc_, index + 1);

        double value;
        dbconvert(dbproc_, coltype,
            (dbdata(dbproc_, index + 1)), (DBFLT8)-1, SYBFLT8,
            (byte*)&value, (DBFLT8)-1);


        return value;

    }
    virtual const float getFloat(int index) const
    {
        throw mapnik::datasource_exception("ResultSet getFloat not implemented");       
    }

    virtual const std::wstring getWString(int index) const
    {
        throw mapnik::datasource_exception("ResultSet getWString not implemented");
    }

    virtual const std::string getString(int index) const
    {      
        const int len  = getFieldLength(index);
        /*
        auto colytpe = dbcoltype(dbproc_, index + 1);
        char* buffer = (char*) alloca(sizeof(char)*len);
               
        dbconvert(dbproc_, colytpe,
            (dbdata(dbproc_, index + 1)), (DBINT)-1, SYBVARCHAR,
            (byte*)buffer, (DBINT)-1);

        */
        char* value = ((char *)dbdata(dbproc_, index + 1));
    
        return std::string(value, len);

    }

    virtual const std::vector<char> getBinary(int index) const
    {           
        int len = dbdatlen(dbproc_, index + 1);
        char* data = (char*)dbdata(dbproc_, index + 1);
        std::vector<char> binvalue(data, data + len);

        return binvalue;    
    }

private:
    DBPROCESS *dbproc_;
    bool is_closed_;
};

#endif // MSSQL_DBLIBRESULTSET_HPP
