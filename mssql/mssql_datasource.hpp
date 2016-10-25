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

#ifndef MSSQL_DATASOURCE_HPP
#define MSSQL_DATASOURCE_HPP

// mapnik
#include <mapnik/attribute.hpp>
#include <mapnik/box2d.hpp>
#include <mapnik/coord.hpp>
#include <mapnik/datasource.hpp>
#include <mapnik/feature.hpp>
#include <mapnik/feature_layer_desc.hpp>
#include <mapnik/params.hpp>
#include <mapnik/query.hpp>
#include <mapnik/unicode.hpp>
#include <mapnik/value_types.hpp>

// boost
#include <boost/optional.hpp>

// stl
#include <memory>
#include <string>
#include <vector>

#include "connection_manager.hpp"
//#include "cursorresultset.hpp"
//#include "resultset.hpp"

//using mapnik::transcoder;
using mapnik::datasource;
using mapnik::feature_style_context_map;
using mapnik::processor_context_ptr;
using mapnik::box2d;
using mapnik::layer_descriptor;
using mapnik::featureset_ptr;
using mapnik::feature_ptr;
using mapnik::query;
using mapnik::parameters;
using mapnik::coord2d;

using CnxPool_ptr = std::shared_ptr<ConnectionManager::PoolType>;

class mssql_datasource : public datasource
{
  public:
    mssql_datasource(const parameters& params);
    ~mssql_datasource();
    mapnik::datasource::datasource_t type() const;
    static const char* name();
    processor_context_ptr get_context(feature_style_context_map&) const;
    featureset_ptr features_with_context(query const& q, processor_context_ptr ctx) const;
    featureset_ptr features(query const& q) const;
    featureset_ptr features_at_point(coord2d const& pt, double tol = 0) const;
    mapnik::box2d<double> envelope() const;
    boost::optional<mapnik::datasource_geometry_t> get_geometry_type() const;
    layer_descriptor get_descriptor() const;

  private:
    std::string sql_bbox(box2d<double> const& env) const;
    std::string populate_tokens(std::string const& sql,
                                double scale_denom,
                                box2d<double> const& env,
                                double pixel_width,
                                double pixel_height,
                                mapnik::attributes const& vars) const;
    std::string populate_tokens(std::string const& sql) const;
    std::shared_ptr<IResultSet> get_resultset(std::shared_ptr<Connection>& conn, std::string const& sql, CnxPool_ptr const& pool, processor_context_ptr ctx = processor_context_ptr()) const;
    static const double FMAX;

    const std::string uri_;
    const std::string username_;
    const std::string password_;
    const std::string table_;
    std::string schema_;
    std::string geometry_table_;
    const std::string geometry_field_;
    std::string key_field_;
    bool wkb_;
    bool use_filter_;
    bool trace_flag_4199_;

    mapnik::value_integer row_limit_;
    std::string geometryColumn_;
    std::string geometryColumnType_;
    mapnik::datasource::datasource_t type_;
    int srid_;
    mutable bool extent_initialized_;
    mutable mapnik::box2d<double> extent_;
    bool simplify_geometries_;
    layer_descriptor desc_;
    std::shared_ptr<Odbc> odbc_instance_;
    ConnectionCreator<Connection> creator_;
    const std::string bbox_token_;
    const std::string scale_denom_token_;
    const std::string pixel_width_token_;
    const std::string pixel_height_token_;
    int pool_max_size_;
    bool persist_connection_;
    bool extent_from_subquery_;
    bool estimate_extent_;
    int max_async_connections_;
    bool asynchronous_request_;
    int intersect_min_scale_;
    int intersect_max_scale_;
    bool key_field_as_attribute_;
    std::string order_by_;
};

#endif // MSSQL_DATASOURCE_HPP
