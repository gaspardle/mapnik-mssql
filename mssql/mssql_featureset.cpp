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

#include "stdafx.h"

#include "mssql_featureset.hpp"
#include "resultset.hpp"
#include "cursorresultset.hpp"

// mapnik
/*#include <mapnik/global.hpp>
#include <mapnik/debug.hpp>
#include <mapnik/wkb.hpp>
#include <mapnik/unicode.hpp>
#include <mapnik/value_types.hpp>
#include <mapnik/feature_factory.hpp>
#include <mapnik/util/conversions.hpp>
#include <mapnik/util/trim.hpp>
#include <mapnik/global.hpp> // for int2net
#include <boost/scoped_array.hpp>*/

#include <cstdint>

// stl
#include <sstream>
#include <string>

using mapnik::geometry_type;
using mapnik::byte;
using mapnik::geometry_utils;
using mapnik::feature_factory;
using mapnik::context_ptr;

mssql_featureset::mssql_featureset(boost::shared_ptr<IResultSet> const& rs,
                                       context_ptr const& ctx,
                                       std::string const& encoding,
                                       bool key_field)
    : rs_(rs),
      ctx_(ctx),
      tr_(new transcoder(encoding)),
      totalGeomSize_(0),
      feature_id_(1),
      key_field_(key_field)
{
}

std::string numeric2string(const char* buf);

feature_ptr mssql_featureset::next()
{
    while (rs_->next())
    {
        // new feature
        unsigned pos = 1;
        feature_ptr feature;
		
        if (key_field_)
        {
            std::string name = rs_->getFieldName(pos);

            // null feature id is not acceptable
            if (rs_->isNull(pos))
            {
                MAPNIK_LOG_WARN(mssql) << "mssql_featureset: null value encountered for key_field: " << name;
                continue;
            }
            // create feature with user driven id from attribute
            int oid = rs_->getTypeOID(pos);
         

            // validation happens of this type at initialization
            mapnik::value_integer val;
            val = rs_->getInt(pos);

            feature = feature_factory::create(ctx_, val);
            // TODO - extend feature class to know
            // that its id is also an attribute to avoid
            // this duplication
            feature->put<mapnik::value_integer>(name,val);
            ++pos;
        }
        else
        {
            // fallback to auto-incrementing id
            feature = feature_factory::create(ctx_, feature_id_);
            ++feature_id_;
        }

        // null geometry is not acceptable
        if (rs_->isNull(0))
        {
            MAPNIK_LOG_WARN(mssql) << "mssql_featureset: null value encountered for geometry";
            continue;
        }

        // parse geometry
       
        std::vector<const char> data = rs_->getBinary(0);    
        int size = data.size();       

        if (!geometry_utils::from_wkb(feature->paths(), &data[0], data.size())){
            continue;
        }

        totalGeomSize_ += size;
        unsigned num_attrs = ctx_->size() + 1;
        for (; pos < num_attrs; ++pos)
        {
            std::string name = rs_->getFieldName(pos);

            // NOTE: we intentionally do not store null here
            // since it is equivalent to the attribute not existing
            if (!rs_->isNull(pos))
            {
                //const char* buf = rs_->getValue(pos);
                const int oid = rs_->getTypeOID(pos);
                switch (oid)
                {
                    case SQL_BIT:
                        feature->put(name, rs_->getInt(pos) != 0);
                    break;
                    case SQL_SMALLINT:
                    case SQL_TINYINT:
                    case SQL_INTEGER:
                    case SQL_BIGINT:
                        feature->put<mapnik::value_integer>(name, rs_->getInt(pos));
                        break;
                    case SQL_FLOAT:
                    case SQL_REAL:
                        feature->put(name, static_cast<double>(rs_->getFloat(pos)));
                        break;                    
                    case SQL_DOUBLE:
                        feature->put(name, rs_->getDouble(pos));               
                        break;
                    case SQL_VARCHAR:
                    case SQL_LONGVARCHAR:
                         feature->put(name, (UnicodeString)tr_->transcode(rs_->getString(pos).c_str()));
                         break;
                    case SQL_WVARCHAR:
                    case SQL_WLONGVARCHAR:
                         feature->put(name, (UnicodeString)tr_->transcode(rs_->getString(pos).c_str()));
                         break;
  
                    default:
                    {
                        MAPNIK_LOG_WARN(mssql) << "mssql_featureset: Unknown type=" << oid;

                        break;
                    }
                }
            }
        }
        return feature;
    }
    return feature_ptr();
}


mssql_featureset::~mssql_featureset()
{
    rs_->close();
}

std::string numeric2string(const char* buf)
{
    std::int16_t ndigits = int2net(buf);
	std::int16_t weight  = int2net(buf+2);
	std::int16_t sign    = int2net(buf+4);
	std::int16_t dscale  = int2net(buf+6);

    boost::scoped_array<boost::int16_t> digits(new std::int16_t[ndigits]);
    for (int n=0; n < ndigits ;++n)
    {
        digits[n] = int2net(buf+8+n*2);
    }

    std::ostringstream ss;

    if (sign == 0x4000) ss << "-";

    int i = std::max(weight,std::int16_t(0));
    int d = 0;

    // Each numeric "digit" is actually a value between 0000 and 9999 stored in a 16 bit field.
    // For example, the number 1234567809990001 is stored as four digits: [1234] [5678] [999] [1].
    // Note that the last two digits show that the leading 0's are lost when the number is split.
    // We must be careful to re-insert these 0's when building the string.

    while ( i >= 0)
    {
        if (i <= weight && d < ndigits)
        {
            // All digits after the first must be padded to make the field 4 characters long
            if (d != 0)
            {
#ifdef _WINDOWS
                int dig = digits[d];
                if (dig < 10)
                {
                    ss << "000"; // 0000 - 0009
                }
                else if (dig < 100)
                {
                    ss << "00";  // 0010 - 0099
                }
                else
                {
                    ss << "0";   // 0100 - 0999;
                }
#else
                switch(digits[d])
                {
                case 0 ... 9:
                    ss << "000"; // 0000 - 0009
                    break;
                case 10 ... 99:
                    ss << "00";  // 0010 - 0099
                    break;
                case 100 ... 999:
                    ss << "0";   // 0100 - 0999
                    break;
                }
#endif
            }
            ss << digits[d++];
        }
        else
        {
            if (d == 0)
                ss <<  "0";
            else
                ss <<  "0000";
        }

        i--;
    }
    if (dscale > 0)
    {
        ss << '.';
        // dscale counts the number of decimal digits following the point, not the numeric digits
        while (dscale > 0)
        {
            int value;
            if (i <= weight && d < ndigits)
                value = digits[d++];
            else
                value = 0;

            // Output up to 4 decimal digits for this value
            if (dscale > 0) {
                ss << (value / 1000);
                value %= 1000;
                dscale--;
            }
            if (dscale > 0) {
                ss << (value / 100);
                value %= 100;
                dscale--;
            }
            if (dscale > 0) {
                ss << (value / 10);
                value %= 10;
                dscale--;
            }
            if (dscale > 0) {
                ss << value;
                dscale--;
            }

            i--;
        }
    }
    return ss.str();
}
