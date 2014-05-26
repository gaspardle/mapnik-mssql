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

#ifndef MSSQL_CURSORRESULTSET_HPP
#define MSSQL_CURSORRESULTSET_HPP

#include <mapnik/debug.hpp>

#include "connection.hpp"
#include "resultset.hpp"

class CursorResultSet : public IResultSet
{
public:
    CursorResultSet(boost::shared_ptr<Connection> const &conn, std::string cursorName, int fetch_count)
        : conn_(conn),
          cursorName_(cursorName),
          fetch_size_(fetch_count),
          is_closed_(false),
          refCount_(new int(1))
    {
        getNextResultSet();
    }

    CursorResultSet(const CursorResultSet& rhs)
        : conn_(rhs.conn_),
          cursorName_(rhs.cursorName_),
          rs_(rhs.rs_),
          fetch_size_(rhs.fetch_size_),
          is_closed_(rhs.is_closed_),
          refCount_(rhs.refCount_)
    {
        (*refCount_)++;
    }

    virtual ~CursorResultSet()
    {
        if (--(*refCount_)==0)
        {
            close();
            delete refCount_,refCount_=0;
        }
    }

    CursorResultSet& operator=(const CursorResultSet& rhs)
    {
        if (this==&rhs) return *this;
        if (--(refCount_)==0)
        {
            close();
            delete refCount_,refCount_=0;
        }
        conn_=rhs.conn_;
        cursorName_=rhs.cursorName_;
        rs_=rhs.rs_;
        refCount_=rhs.refCount_;
        fetch_size_=rhs.fetch_size_;
        is_closed_ = false;
        (*refCount_)++;
        return *this;
    }

    virtual void close()
    {
        if (!is_closed_)
        {
            rs_.reset();
            
            is_closed_ = true;
        }
    }

    virtual int getNumFields() const
    {
        return rs_->getNumFields();
    }

    virtual bool next()
    {
        if (rs_->next()) {
            return true;
        } else if (rs_->size() == 0) {
            return false;
        } else {
            getNextResultSet();
            return rs_->next();
        }
    }

    virtual const std::string getFieldName(int index) const
    {
        return rs_->getFieldName(index);
    }

    virtual int getFieldLength(int index) const
    {
        return rs_->getFieldLength(index);
    }

    virtual int getFieldLength(const char* name) const
    {
        return rs_->getFieldLength(name);
    }

    virtual int getTypeOID(int index) const
    {
        return rs_->getTypeOID(index);
    }

    virtual int getTypeOID(const char* name) const
    {
        return rs_->getTypeOID(name);
    }

    virtual bool isNull(int index) const
    {
        return rs_->isNull(index);
    }

    virtual const std::string getString(int index) const
    {
        return rs_->getString(index);
    }
    virtual const int getInt(int index) const
    {
        return rs_->getInt(index);
    }
    virtual const float getFloat(int index) const
    {
        return rs_->getFloat(index);
    }
    virtual const double getDouble(int index) const
    {
        return rs_->getDouble(index);
    }
    virtual const char* getValue(int index) const
    {
        return rs_->getValue(index);
    }
    virtual const std::vector<const char> getBinary(int index) const{
        return rs_->getBinary(index);
    }
    virtual const char* getValue(const char* name) const
    {
        return rs_->getValue(name);
    }

private:
    void getNextResultSet()
    {
        std::ostringstream s;
        s << "FETCH FORWARD " << fetch_size_ << " FROM " << cursorName_;

        MAPNIK_LOG_DEBUG(mssql) << "mssql_cursor_resultset: " << s.str();

        rs_ = conn_->executeQuery(s.str());
        is_closed_ = false;

        MAPNIK_LOG_DEBUG(mssql) << "mssql_cursor_resultset: FETCH result (" << cursorName_ << "): " << rs_->size() << " rows";
    }

    boost::shared_ptr<Connection> conn_;
    std::string cursorName_;
    boost::shared_ptr<ResultSet> rs_;
    int fetch_size_;
    bool is_closed_;
    int *refCount_;
};

#endif // MSSQL_CURSORRESULTSET_HPP