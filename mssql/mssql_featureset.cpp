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
#include "geoclr_reader.hpp"

// mapnik
/*#include <mapnik/global.hpp>
#include <mapnik/debug.hpp>
#include <mapnik/wkb.hpp>
#include <mapnik/unicode.hpp>
#include <mapnik/value_types.hpp>
#include <mapnik/feature_factory.hpp>
#include <mapnik/util/conversions.hpp>
#include <mapnik/util/trim.hpp>
#include <mapnik/global.hpp> // for int2net*/

#include <cstdint>

// stl
#include <sstream>
#include <string>
#include <memory>


using mapnik::geometry_utils;
using mapnik::feature_factory;
using mapnik::context_ptr;

mssql_featureset::mssql_featureset(std::shared_ptr<IResultSet> const& rs,
                                     context_ptr const& ctx,
                                     bool wkb,
                                     bool is_sqlgeography,
                                     bool key_field )
    : rs_(rs),
      ctx_(ctx),
	  wkb_(wkb),
      is_sqlgeography_(is_sqlgeography),
	  tr_ucs2_(new transcoder("UTF-16LE")),
	  tr_(new transcoder("UTF-8")),
      totalGeomSize_(0),
      feature_id_(1),
      key_field_(key_field)
{
}

static inline std::string numeric2string(const char* buf);

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
        std::vector<char> data = rs_->getBinary(0);
        size_t size = data.size();
				
		mapnik::geometry::geometry<double> geometry;
		if (wkb_) {
			geometry = geometry_utils::from_wkb(&data[0], size);
		}
		else {			
			geometry = from_geoclr(&data[0], size, is_sqlgeography_);
		}
		feature->set_geometry(std::move(geometry));

        totalGeomSize_ += size;
        unsigned num_attrs = ctx_->size() + 1;
        for (; pos < num_attrs; ++pos)
        {
            std::string name = rs_->getFieldName(pos);

            // NOTE: we intentionally do not store null here
            // since it is equivalent to the attribute not existing
            if (!rs_->isNull(pos))
            {              
                const int oid = rs_->getTypeOID(pos);
				
                switch (oid)
                {
                    case SQL_BIT:
                        feature->put(name, rs_->getInt(pos) != 0);
                    break;
                    case SQL_SMALLINT:
                    case SQL_TINYINT:
                    case SQL_INTEGER:
                        feature->put<mapnik::value_integer>(name, rs_->getInt(pos));
                        break;
                    case SQL_BIGINT:
                        feature->put<mapnik::value_integer>(name, rs_->getBigInt(pos));
                        break;
                    case SQL_FLOAT:
                    case SQL_REAL:
                        feature->put(name, static_cast<double>(rs_->getFloat(pos)));
                        break;                    
                    case SQL_DOUBLE:
                        feature->put(name, rs_->getDouble(pos));
                        break;
                    case SQL_DECIMAL:
                    case SQL_NUMERIC:
                        feature->put(name, rs_->getDouble(pos));
                        break;
					case SQL_VARCHAR:
                    case SQL_LONGVARCHAR:
                         feature->put(name, (UnicodeString)tr_->transcode(rs_->getString(pos).c_str()));
                         break;
                    case SQL_WVARCHAR:
					case SQL_WLONGVARCHAR:
                    {
						//feature->put(name, (UnicodeString)tr_->transcode(rs_->getString(pos).c_str()));

						//easier than to deal with wchar on each platforms
                        auto stringbin = rs_->getBinary(pos);
						feature->put(name, (UnicodeString)tr_ucs2_->transcode(stringbin.data(), stringbin.size()));
						break;
                    }
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
