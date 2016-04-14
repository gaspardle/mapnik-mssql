
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

#ifndef MSSQL_FEATURESET_HPP
#define MSSQL_FEATURESET_HPP

// mapnik
///#include <mapnik/box2d.hpp>
///#include <mapnik/datasource.hpp>
#include <mapnik/feature.hpp>
#include <mapnik/featureset.hpp>
#include <mapnik/unicode.hpp>
#include <memory>

using mapnik::Featureset;
using mapnik::box2d;
using mapnik::feature_ptr;
using mapnik::transcoder;
using mapnik::context_ptr;

class IResultSet;

class mssql_featureset : public mapnik::Featureset
{
  public:
    mssql_featureset(std::shared_ptr<IResultSet> const& rs,
                     context_ptr const& ctx,
                     bool wkb,
                     bool is_sqlgeography,
                     bool key_field,
                     bool key_field_as_attribute);
    feature_ptr next();
    ~mssql_featureset();

  private:
    std::shared_ptr<IResultSet> rs_;
    context_ptr ctx_;
    bool wkb_;
    bool is_sqlgeography_;
    const std::unique_ptr<mapnik::transcoder> tr_ucs2_;
    const std::unique_ptr<mapnik::transcoder> tr_;
    size_t totalGeomSize_;
    mapnik::value_integer feature_id_;
    bool key_field_;
    bool key_field_as_attribute_;

    template <typename T>
    inline void putIfNotNull(feature_ptr feature, mapnik::context_type::key_type const& key, T const& val)
    {
        if (val)
        {
            feature->put(key, *val);
        }
    }
};

#endif // MSSQL_FEATURESET_HPP
