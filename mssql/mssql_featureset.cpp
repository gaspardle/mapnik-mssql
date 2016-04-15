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

#include "geoclr_reader.hpp"
#include "mssql_featureset.hpp"
#include "resultset.hpp"

// mapnik
#include <mapnik/debug.hpp>
#include <mapnik/feature_factory.hpp>
///#include <mapnik/global.hpp> // for int2net
#include <mapnik/unicode.hpp>
///#include <mapnik/util/conversions.hpp>
#include <mapnik/value_types.hpp>
#include <mapnik/wkb.hpp>

#include <cstdint>

// stl
#include <memory>
#include <sstream>
#include <string>

using mapnik::geometry_utils;
using mapnik::feature_factory;
using mapnik::context_ptr;

mssql_featureset::mssql_featureset(std::shared_ptr<IResultSet> const& rs,
                                   context_ptr const& ctx,
                                   bool wkb,
                                   bool is_sqlgeography,
                                   bool key_field,
                                   bool key_field_as_attribute)
    : rs_(rs),
      ctx_(ctx),
      wkb_(wkb),
      is_sqlgeography_(is_sqlgeography),
      tr_ucs2_(new transcoder("UTF-16LE")),
      tr_(new transcoder("UTF-8")),
      totalGeomSize_(0),
      feature_id_(1),
      key_field_(key_field),
      key_field_as_attribute_(key_field_as_attribute)
{
}

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

            boost::optional<int> id = rs_->getInt(pos);
            // null feature id is not acceptable
            if (!id)
            {
                MAPNIK_LOG_WARN(mssql) << "mssql_featureset: null value encountered for key_field: " << name;
                continue;
            }

            // validation happens of this type at initialization
            mapnik::value_integer val;
            val = id.get();

            feature = feature_factory::create(ctx_, val);
            if (key_field_as_attribute_)
            {
                feature->put<mapnik::value_integer>(name, val);
            }
            ++pos;
        }
        else
        {
            // fallback to auto-incrementing id
            feature = feature_factory::create(ctx_, feature_id_);
            ++feature_id_;
        }

        // parse geometry
        std::vector<char> data = rs_->getBinary(0);
        size_t size = data.size();

        // null geometry is not acceptable
        if (size == 0)
        {
            MAPNIK_LOG_WARN(mssql) << "mssql_featureset: null value encountered for geometry";
            continue;
        }

        mapnik::geometry::geometry<double> geometry;
        if (wkb_)
        {
            geometry = geometry_utils::from_wkb(&data[0], size);
        }
        else
        {
            geometry = from_geoclr(&data[0], size, is_sqlgeography_);
        }
        feature->set_geometry(std::move(geometry));

        totalGeomSize_ += size;

        unsigned num_attrs = ctx_->size() + 1;
        if (!key_field_as_attribute_)
        {
            num_attrs++;
        }

        for (; pos < num_attrs; ++pos)
        {
            std::string name = rs_->getFieldName(pos);

            // NOTE: we intentionally do not store null here
            // since it is equivalent to the attribute not existing
            if (true /*!rs_->isNull(pos)*/)
            {
                const int oid = rs_->getTypeOID(pos);

                switch (oid)
                {
                case SQL_BIT:
                {
                    auto bit = rs_->getInt(pos);
                    if (bit)
                    {
                        feature->put(name, *bit != 0);
                    }
                    break;
                }
                case SQL_SMALLINT:
                case SQL_TINYINT:
                case SQL_INTEGER:
                    putIfNotNull(feature, name, rs_->getInt(pos));
                    //feature->put<mapnik::value_integer>(name, rs_->getInt(pos));
                    break;
                case SQL_BIGINT:
                    putIfNotNull(feature, name, rs_->getBigInt(pos));
                    //feature->put<mapnik::value_integer>(name, rs_->getBigInt(pos));
                    break;
                case SQL_FLOAT:
                case SQL_REAL:
                    putIfNotNull(feature, name, (rs_->getFloat(pos)));
                    //feature->put(name, static_cast<double>(rs_->getFloat(pos)));
                    break;
                case SQL_DOUBLE:
                    putIfNotNull(feature, name, rs_->getDouble(pos));
                    //feature->put(name, rs_->getDouble(pos));
                    break;
                case SQL_DECIMAL:
                case SQL_NUMERIC:
                    putIfNotNull(feature, name, rs_->getDouble(pos));
                    //feature->put(name, rs_->getDouble(pos));
                    break;
                case SQL_CHAR:
                case SQL_VARCHAR:
                case SQL_LONGVARCHAR:
                    feature->put(name, (UnicodeString)tr_->transcode(rs_->getString(pos).c_str()));
                    break;
                case SQL_WCHAR:
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
