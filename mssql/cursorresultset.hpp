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
#include <memory>
class CursorResultSet : public IResultSet, private mapnik::util::noncopyable
{
  public:
    CursorResultSet(std::shared_ptr<Pool<Connection, ConnectionCreator>> const& pool,
                    std::shared_ptr<Connection> const& conn,
                    std::string const& sql)
        : pool_(pool),
          conn_(conn),
          sql_(sql),
          is_closed_(false)
    {
    }

    virtual ~CursorResultSet()
    {
        close();
    }

    virtual void close()
    {
        if (!is_closed_)
        {
            rs_.reset();

            conn_->close();
            is_closed_ = true;
            conn_.reset();
        }
    }

    virtual int getNumFields() const
    {
        return rs_->getNumFields();
    }

    virtual bool next()
    {
        if (!rs_)
        {
            getNextResultSet();
        }

        if (rs_->next())
        {
            return true;
        }
        else
        {
            close();
            return false;
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
    virtual const std::wstring getWString(int index) const
    {
        return rs_->getWString(index);
    }
    virtual const boost::optional<int> getInt(int index) const
    {
        return rs_->getInt(index);
    }
    virtual const boost::optional<long long> getBigInt(int index) const
    {
        return rs_->getBigInt(index);
    }
    virtual const boost::optional<float> getFloat(int index) const
    {
        return rs_->getFloat(index);
    }
    virtual const boost::optional<double> getDouble(int index) const
    {
        return rs_->getDouble(index);
    }

    virtual const std::vector<char> getBinary(int index) const
    {
        return rs_->getBinary(index);
    }

  private:
    void getNextResultSet()
    {
        if (!conn_)
        {
            conn_ = pool_->borrowObject();
            if (!conn_ || !conn_->isOK())
            {
                throw mapnik::datasource_exception("Mssql Plugin: bad connection");
            }
        }
        rs_ = conn_->executeQuery(sql_.c_str());
        is_closed_ = false;
    }
    std::shared_ptr<Pool<Connection, ConnectionCreator>> pool_;
    std::shared_ptr<Connection> conn_;
    std::shared_ptr<ResultSet> rs_;

    std::string sql_;
    bool is_closed_;
};

#endif // MSSQL_CURSORRESULTSET_HPP
