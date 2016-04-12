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

//#include "stdafx.h"

#include "connection_manager.hpp"
#include "mssql_datasource.hpp"
#include "mssql_featureset.hpp"
#include "asyncresultset.hpp"


// mapnik

#include <mapnik/debug.hpp>
#include <mapnik/global.hpp>
#include <mapnik/boolean.hpp>
#include <mapnik/sql_utils.hpp>
#include <mapnik/util/conversions.hpp>
#include <mapnik/timer.hpp>
#include <mapnik/value_types.hpp>

// boost
#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>

// stl
#include <memory>
#include <string>
#include <algorithm>
#include <set>
#include <sstream>
#include <iomanip>

DATASOURCE_PLUGIN(mssql_datasource)

const double mssql_datasource::FMAX = std::numeric_limits<float>::max();
//const std::string mssql_datasource::GEOMETRY_COLUMNS = "geometry_columns";
//const std::string mssql_datasource::SPATIAL_REF_SYS = "spatial_ref_system";

using std::shared_ptr;
using mapnik::attribute_descriptor;

mssql_datasource::mssql_datasource(parameters const& params)
    : datasource(params),
      table_(*params.get<std::string>("table", "")),
      schema_(""),
      order_by_(*params.get<std::string>("order_by", "")),
      geometry_table_(*params.get<std::string>("geometry_table", "")),
      geometry_field_(*params.get<std::string>("geometry_field", "")),
      key_field_(*params.get<std::string>("key_field", "")),

      row_limit_(*params.get<mapnik::value_integer>("row_limit", 0)),
      type_(datasource::Vector),
      srid_(*params.get<mapnik::value_integer>("srid", 0)),
      extent_initialized_(false),
      simplify_geometries_(false),
      desc_(mssql_datasource::name(), "utf-8"),
      creator_(params.get<std::string>("connection_string"),
               params.get<std::string>("driver", "{SQL Server Native Client 11.0}"),
               params.get<std::string>("host"),
               params.get<std::string>("port"),
               params.get<std::string>("dbname"),
               params.get<std::string>("user"),
               params.get<std::string>("password"),
               params.get<std::string>("connect_timeout", "4")),
      bbox_token_("!bbox!"),
      scale_denom_token_("!scale_denominator!"),
      pixel_width_token_("!pixel_width!"),
      pixel_height_token_("!pixel_height!"),
      pool_max_size_(*params_.get<mapnik::value_integer>("max_size", 10)),
      persist_connection_(*params.get<mapnik::boolean_type>("persist_connection", true)),
      extent_from_subquery_(*params.get<mapnik::boolean_type>("extent_from_subquery", false)),
      max_async_connections_(*params_.get<mapnik::value_integer>("max_async_connection", 1)),
      asynchronous_request_(false),
      // params below are for testing purposes only and may be removed at any time
      intersect_min_scale_(*params.get<mapnik::value_integer>("intersect_min_scale", 0)),
      intersect_max_scale_(*params.get<mapnik::value_integer>("intersect_max_scale", 0)),
      key_field_as_attribute_(*params.get<mapnik::boolean_type>("key_field_as_attribute", true)),

      wkb_(*params.get<mapnik::boolean_type>("wkb", false)),
      use_filter_(*params.get<mapnik::boolean_type>("use_filter", false)),
      trace_flag_4199_(*params.get<mapnik::boolean_type>("trace_flag_4199", false))

{
#ifdef MAPNIK_STATS
    mapnik::progress_timer __stats__(std::clog, "mssql_datasource::init");
#endif
    if (table_.empty())
    {
        throw mapnik::datasource_exception("Mssql Plugin: missing <table> parameter");
    }

    boost::optional<std::string> ext = params.get<std::string>("extent");
    if (ext && !ext->empty())
    {
        extent_initialized_ = extent_.from_string(*ext);
    }

    // NOTE: In multithread environment, pool_max_size_ should be
    // max_async_connections_ * num_threads
    if (max_async_connections_ > 1)
    {
        if (max_async_connections_ > pool_max_size_)
        {
            std::ostringstream err;
            err << "MSSQL Plugin: Error: 'max_async_connections ("
                << max_async_connections_ << ") must be <= max_size(" << pool_max_size_ << ")";
            throw mapnik::datasource_exception(err.str());
        }
        asynchronous_request_ = true;
    }

    boost::optional<mapnik::value_integer> initial_size = params.get<mapnik::value_integer>("initial_size", 1);
    boost::optional<mapnik::boolean_type> autodetect_key_field = params.get<mapnik::boolean_type>("autodetect_key_field", false);
    boost::optional<mapnik::boolean_type> estimate_extent = params.get<mapnik::boolean_type>("estimate_extent", false);
    estimate_extent_ = estimate_extent && *estimate_extent;
    boost::optional<mapnik::boolean_type> simplify_opt = params.get<mapnik::boolean_type>("simplify_geometries", false);
    simplify_geometries_ = simplify_opt && *simplify_opt;

    static bool const quiet_unused = (Odbc::InitOdbc(), true);   

    ConnectionManager::instance().registerPool(creator_, *initial_size, pool_max_size_);
    CnxPool_ptr pool = ConnectionManager::instance().getPool(creator_.id());
    if (pool)
    {
        shared_ptr<Connection> conn = pool->borrowObject();
        if (!conn)
        {
            return;
        }

        if (conn->isOK())
        {

            if (geometry_table_.empty())
            {
                geometry_table_ = mapnik::sql_utils::table_from_sql(table_);
                boost::algorithm::replace_all(geometry_table_, "\r", " ");
            }

            std::string::size_type idx = geometry_table_.find_last_of('.');
            if (idx != std::string::npos)
            {
                schema_ = geometry_table_.substr(0, idx);
                geometry_table_ = geometry_table_.substr(idx + 1);
            }

            // If we do not know both the geometry_field and the srid
            // then first attempt to fetch the geometry name from a geometry_columns entry.
            // This will return no records if we are querying a bogus table returned
            // from the simplistic table parsing in table_from_sql() or if
            // the table parameter references a table, view, or subselect not
            // registered in the geometry columns.
            geometryColumn_ = geometry_field_;
            if (geometryColumn_.empty() || srid_ == 0 || geometryColumnType_.empty())
            {
#ifdef MAPNIK_STATS
                mapnik::progress_timer __stats2__(std::clog, "mssql_datasource::init(get_srid_and_geometry_column)");
#endif
                std::ostringstream s;
                bool check_srid = !geometry_field_.empty() && srid_ == 0;
                try
                {
                    s << "SELECT column_name, data_type";
                    if (check_srid)
                    {
                        s << ", (SELECT TOP 1 ["
                          << geometry_field_ << "].STSrid FROM [" << geometry_table_
                          << "] WHERE ["
                          << geometry_field_ << "] IS NOT NULL)";
                    }
                    s << " FROM "
                      << "information_schema.columns "
                      << " WHERE (data_type = 'geometry' OR data_type = 'geography') AND table_name='"
                      << mapnik::sql_utils::unquote_double(geometry_table_)
                      << "'";

                    if (!schema_.empty())
                    {
                        s << " AND table_schema='"
                          << mapnik::sql_utils::unquote_double(schema_)
                          << "'";
                    }
                    if (!geometry_field_.empty())
                    {
                        s << " AND column_name='"
                          << mapnik::sql_utils::unquote_double(geometry_field_)
                          << "'";
                    }

                    shared_ptr<ResultSet> rs = conn->executeQuery(s.str());
                    if (rs->next())
                    {
                        if (geometryColumn_.empty())
                        {
                            geometryColumn_ = rs->getString(0);
                        }
                        geometryColumnType_ = rs->getString(1);
                        if (check_srid)
                        {
                            srid_ = *rs->getInt(2);
                        }
                    }
                    rs->close();
                }
                catch (mapnik::datasource_exception const& ex)
                {

                    // let this pass on query error and use the fallback below
                    MAPNIK_LOG_WARN(mssql) << "mssql_datasource: metadata query failed: " << ex.what();
                }

                // If we still do not know the srid then we can try to fetch
                // it from the 'table_' parameter, which should work even if it is
                // a subselect as long as we know the geometry_field to query
                if (!geometryColumn_.empty() && srid_ <= 0)
                {
                    s.str("");

                    s << "SELECT TOP 1 ([" << geometryColumn_ << "]).STSrid AS srid FROM "
                      << populate_tokens(table_) << " WHERE [" << geometryColumn_ << "] IS NOT NULL;";

                    shared_ptr<ResultSet> rs = conn->executeQuery(s.str());
                    if (rs->next())
                    {
                        srid_ = *rs->getInt(0);
                    }
                    rs->close();
                }
            }

            // detect primary key
            if (*autodetect_key_field && key_field_.empty())
            {
                throw mapnik::datasource_exception("autodetect_key_field  detect key_field_: " + key_field_);
#ifdef MAPNIK_STATS
                mapnik::progress_timer __stats2__(std::clog, "mssql_datasource::bind(get_primary_key)");
#endif

                std::ostringstream s;
                s << "SELECT kcu.COLUMN_NAME"
                  "from INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc "
                  "join INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu "
                  "on kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA "
                  "and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
                  "and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA "
                  "and kcu.TABLE_NAME = tc.TABLE_NAME "
                  "WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
                  "AND kcu.TABLE_NAME="
                  << "'" << mapnik::sql_utils::unquote_double(geometry_table_) << "' ";

                if (!schema_.empty())
                {
                    s << "AND kcu.TABLE_SCHEMA='"
                      << mapnik::sql_utils::unquote_double(schema_)
                      << "' ";
                }
                s << "ORDER BY kcu.ORDINAL_POSITION";

                shared_ptr<ResultSet> rs_key = conn->executeQuery(s.str());
                if (rs_key->next())
                {
                    std::string key_field_string = rs_key->getString(0);
                    if (!key_field_string.empty())
                    {
                        key_field_ = key_field_string;

                        MAPNIK_LOG_DEBUG(mssql) << "mssql_datasource: auto-detected key field of '"
                                                << key_field_ << "' on table '" << geometry_table_ << "'";
                    }
                }
                rs_key->close();
            }

            // if a globally unique key field/primary key is required
            // but still not known at this point, then throw
            if (*autodetect_key_field && key_field_.empty())
            {
                throw mapnik::datasource_exception(std::string("MSSQL Plugin: Error: primary key required")
                                                   + " but could not be detected for table '" +
                                                   geometry_table_ + "', please supply 'key_field' option to specify field to use for primary key");
            }

            if (srid_ == 0)
            {
                srid_ = -1;

                MAPNIK_LOG_DEBUG(mssql) << "mssql_datasource: Table " << table_ << " is using SRID=" << srid_;
            }

            // At this point the geometry_field may still not be known
            // but we'll catch that where more useful...
            MAPNIK_LOG_DEBUG(mssql) << "mssql_datasource: Using SRID=" << srid_;
            MAPNIK_LOG_DEBUG(mssql) << "mssql_datasource: Using geometry_column=" << geometryColumn_;

            // collect attribute desc
#ifdef MAPNIK_STATS
            mapnik::progress_timer __stats2__(std::clog, "mssql_datasource::bind(get_column_description)");
#endif

            std::ostringstream s;
            s << "SELECT TOP 0 * FROM " << populate_tokens(table_);

            shared_ptr<ResultSet> rs = conn->executeQuery(s.str());
            int count = rs->getNumFields();
            bool found_key_field = false;
            for (int i = 0; i < count; ++i)
            {
                std::string fld_name = rs->getFieldName(i);
                int type_oid = rs->getTypeOID(i);

                // validate type of key_field
                if (!found_key_field && !key_field_.empty() && fld_name == key_field_)
                {
                    if (type_oid == SQL_SMALLINT || type_oid == SQL_TINYINT || type_oid == SQL_INTEGER || type_oid == SQL_BIGINT)
                    {
                        found_key_field = true;
                        if (key_field_as_attribute_)
                        {
                            desc_.add_descriptor(attribute_descriptor(fld_name, mapnik::Integer));
                        }
                    }
                    else
                    {
                        std::ostringstream error_s;
                        error_s << "invalid type '";

                        error_s << "oid:" << type_oid << "'";

                        error_s << " for key_field '" << fld_name << "' - "
                                << "must be an integer primary key";

                        throw mapnik::datasource_exception(error_s.str());
                    }
                }
                else
                {
                    switch (type_oid)
                    {
                    case SQL_BIT:    // bool
                        desc_.add_descriptor(attribute_descriptor(fld_name, mapnik::Boolean));
                        break;
                    case SQL_SMALLINT:
                    case SQL_TINYINT:
                    case SQL_INTEGER:
                    case SQL_BIGINT:
                        desc_.add_descriptor(attribute_descriptor(fld_name, mapnik::Integer));
                        break;
                    case SQL_FLOAT:
                    case SQL_REAL:
                    case SQL_DOUBLE:
                    case SQL_NUMERIC:
                    case SQL_DECIMAL:
                        desc_.add_descriptor(attribute_descriptor(fld_name, mapnik::Double));
                        break;
                    case SQL_VARCHAR:
                    case SQL_LONGVARCHAR:
                    case SQL_WVARCHAR:
                    case SQL_WLONGVARCHAR:
                        desc_.add_descriptor(attribute_descriptor(fld_name, mapnik::String));
                        break;
                    default: // should not get here
#ifdef MAPNIK_LOG
                        MAPNIK_LOG_WARN(mssql) << "mssql_datasource: Unknown type=" <<
                                               " (oid:" << type_oid << ")";
#endif
                        break;
                    }
                }
            }

            rs->close();

        }

        // Close explicitly the connection so we can 'fork()' without sharing open connections
        conn->close();
        
        // Finally, add unique metadata to layer descriptor
        mapnik::parameters & extra_params = desc_.get_extra_parameters();
        // explicitly make copies of values due to https://github.com/mapnik/mapnik/issues/2651
        extra_params["srid"] = srid_;
        if (!key_field_.empty())
        {
            extra_params["key_field"] = key_field_;
        }

    }
}

mssql_datasource::~mssql_datasource()
{
    if (!persist_connection_)
    {
        CnxPool_ptr pool = ConnectionManager::instance().getPool(creator_.id());
        if (pool)
        {
            try
            {
                shared_ptr<Connection> conn = pool->borrowObject();
                if (conn)
                {
                    conn->close();
                }
            }
            catch (mapnik::datasource_exception const& ex)
            {
                // happens when borrowObject tries to
                // create a new connection and fails.
                // In turn, new connection would be needed
                // when our broke and was thus no good to
                // be borrowed
                // See https://github.com/mapnik/mapnik/issues/2191
            }
        }
    }
    
    Odbc::FreeOdbc();
}

const char * mssql_datasource::name()
{
    return "mssql";
}

mapnik::datasource::datasource_t mssql_datasource::type() const
{
    return type_;
}

layer_descriptor mssql_datasource::get_descriptor() const
{
    return desc_;
}

std::string mssql_datasource::sql_bbox(box2d<double> const& env) const
{
    std::ostringstream b;


    b << geometryColumnType_ << "::STGeomFromText('POLYGON((";
    b << std::setprecision(16);
    b << env.minx() << " " << env.miny() << ",";
    b << env.maxx() << " " << env.miny() << ",";
    b << env.maxx() << " " << env.maxy() << ",";
    b << env.minx() << " " << env.maxy() << ",";
    b << env.minx() << " " << env.miny() << "))', " << srid_ << ")";

    return b.str();
}

std::string mssql_datasource::populate_tokens(std::string const& sql) const
{
    std::string populated_sql = sql;

    if (boost::algorithm::icontains(sql, bbox_token_))
    {
        box2d<double> max_env(-1.0 * FMAX, -1.0 * FMAX, FMAX, FMAX);
        const std::string max_box = sql_bbox(max_env);
        boost::algorithm::replace_all(populated_sql, bbox_token_, max_box);
    }

    if (boost::algorithm::icontains(sql, scale_denom_token_))
    {
        std::ostringstream ss;
        ss << FMAX;
        boost::algorithm::replace_all(populated_sql, scale_denom_token_, ss.str());
    }

    if (boost::algorithm::icontains(sql, pixel_width_token_))
    {
        boost::algorithm::replace_all(populated_sql, pixel_width_token_, "0");
    }

    if (boost::algorithm::icontains(sql, pixel_height_token_))
    {
        boost::algorithm::replace_all(populated_sql, pixel_height_token_, "0");
    }

    return populated_sql;
}

std::string mssql_datasource::populate_tokens(
    std::string const& sql,
    double scale_denom,
    box2d<double> const& env,
    double pixel_width,
    double pixel_height,
    mapnik::attributes const& vars) const
{
    std::string populated_sql = sql;
    std::string box = sql_bbox(env);

    if (boost::algorithm::icontains(populated_sql, scale_denom_token_))
    {
        std::ostringstream ss;
        ss << scale_denom;
        boost::algorithm::replace_all(populated_sql, scale_denom_token_, ss.str());
    }

    if (boost::algorithm::icontains(sql, pixel_width_token_))
    {
        std::ostringstream ss;
        ss << pixel_width;
        boost::algorithm::replace_all(populated_sql, pixel_width_token_, ss.str());
    }

    if (boost::algorithm::icontains(sql, pixel_height_token_))
    {
        std::ostringstream ss;
        ss << pixel_height;
        boost::algorithm::replace_all(populated_sql, pixel_height_token_, ss.str());
    }

    if (boost::algorithm::icontains(populated_sql, bbox_token_))
    {
        boost::algorithm::replace_all(populated_sql, bbox_token_, box);
        return populated_sql;
    }
    else
    {
        std::ostringstream s;

        if ((intersect_min_scale_ > 0 && (scale_denom <= intersect_min_scale_)) ||
            !(intersect_max_scale_ > 0 && (scale_denom >= intersect_max_scale_))
            )
        {
            s << " WHERE ";
            if (wkb_)
            {
                s << "[" << geometryColumn_ << "].STIsValid() = 1 AND ";
            }
            if (use_filter_) {
                s << "[" << geometryColumn_ << "].Filter(" << box << ") = 1";
            }
            else {
				s << "[" << geometryColumn_ << "].STIntersects(" << box << ") = 1";
			}
			
        }
        else
        {
            // do no bbox restriction
        }       

        return populated_sql + s.str();
    }
}


shared_ptr<IResultSet> mssql_datasource::get_resultset(shared_ptr<Connection> &conn, std::string const& sql, CnxPool_ptr const& pool, processor_context_ptr ctx) const
{

    if (!ctx)
    {

        return std::make_shared<CursorResultSet>(pool, conn, sql);
    }
    else
    {
        // asynchronous requests

        shared_ptr<mssql_processor_context> pgis_ctxt = std::static_pointer_cast<mssql_processor_context>(ctx);
        if (conn)
        {
            // lauch async req & create asyncresult with conn
            conn->executeAsyncQuery(sql);
            return std::make_shared<AsyncResultSet>(pgis_ctxt, pool, conn, sql);
        }
        else
        {
            // create asyncresult  with  null connection
            shared_ptr<AsyncResultSet> res = std::make_shared<AsyncResultSet>(pgis_ctxt, pool, conn, sql);
            pgis_ctxt->add_request(res);
            return res;
        }
    }
}

processor_context_ptr mssql_datasource::get_context(feature_style_context_map & ctx) const
{
    if (!asynchronous_request_)
    {
        return processor_context_ptr();
    }

    std::string ds_name(name());
    feature_style_context_map::const_iterator itr = ctx.find(ds_name);
    if (itr != ctx.end())
    {
        return itr->second;
    }
    else
    {
        return ctx.emplace(ds_name, std::make_shared<mssql_processor_context>()).first->second;
    }
}

featureset_ptr mssql_datasource::features(query const& q) const
{
    // if the driver is in asynchronous mode, return the appropriate fetaures
    if (asynchronous_request_)
    {
        return features_with_context(q, std::make_shared<mssql_processor_context>());
    }
    else
    {
        return features_with_context(q, processor_context_ptr());
    }
}

featureset_ptr mssql_datasource::features_with_context(query const& q, processor_context_ptr proc_ctx) const
{

#ifdef MAPNIK_STATS
    mapnik::progress_timer __stats__(std::clog, "mssql_datasource::features_with_context");
#endif


    box2d<double> const& box = q.get_bbox();
    double scale_denom = q.scale_denominator();

    CnxPool_ptr pool = ConnectionManager::instance().getPool(creator_.id());

    if (pool)
    {
        shared_ptr<Connection> conn;

        if (asynchronous_request_)
        {
            // limit use to num_async_request_ => if reached don't borrow the last connexion object
            shared_ptr<mssql_processor_context> pgis_ctxt = std::static_pointer_cast<mssql_processor_context>(proc_ctx);
            if (pgis_ctxt->num_async_requests_ < max_async_connections_)
            {
                conn = pool->borrowObject();
                pgis_ctxt->num_async_requests_++;
            }
        }
        else
        {
            // Always get a connection in synchronous mode
            conn = pool->borrowObject();
            /*if (!conn)
            {
            	throw mapnik::datasource_exception("Mssql Plugin: Null connection");
            }*/
        }


        if (geometryColumn_.empty())
        {
            std::ostringstream s_error;
            s_error << "MSSQL: geometry name lookup failed for table '";

            if (!schema_.empty())
            {
                s_error << schema_ << ".";
            }
            s_error << geometry_table_
                    << "'. Please manually provide the 'geometry_field' parameter or add an entry "
                    << "in the geometry_columns for '";

            if (!schema_.empty())
            {
                s_error << schema_ << ".";
            }
            s_error << geometry_table_ << "'.";

            throw mapnik::datasource_exception(s_error.str());
        }

        std::ostringstream s;

        const double px_gw = 1.0 / std::get<0>(q.resolution());
        const double px_gh = 1.0 / std::get<1>(q.resolution());

        s << "SELECT ";
        if (row_limit_ > 0)
        {
            s << " TOP " << row_limit_;
        }

        s << "[" << geometryColumn_ << "]";

        if (simplify_geometries_)
        {
            s << ".Reduce(";
            // 1/20 of pixel seems to be a good compromise to avoid
            // drop of collapsed polygons.
            // See https://github.com/mapnik/mapnik/issues/1639
            const double tolerance = std::min(px_gw, px_gh) / 20.0;
            s << tolerance << ")";
        }

        if (wkb_)
        {
            s << ".STAsBinary()";
        }

        s << " AS geom";

        mapnik::context_ptr ctx = std::make_shared<mapnik::context_type>();
        std::set<std::string> const& props = q.property_names();
        std::set<std::string>::const_iterator pos = props.begin();
        std::set<std::string>::const_iterator end = props.end();

        if (!key_field_.empty())
        {
            mapnik::sql_utils::quote_attr(s, key_field_);
            if (key_field_as_attribute_)
            {
                ctx->push(key_field_);
            }

            for (; pos != end; ++pos)
            {
                if (*pos != key_field_)
                {
                    mapnik::sql_utils::quote_attr(s, *pos);
                    ctx->push(*pos);
                }
            }
        }
        else
        {
            for (; pos != end; ++pos)
            {
                mapnik::sql_utils::quote_attr(s, *pos);
                ctx->push(*pos);
            }
        }

        std::string table_with_bbox = populate_tokens(table_, scale_denom, box, px_gw, px_gh, q.variables());

        s << " FROM " << table_with_bbox;

        if (!order_by_.empty())
        {
            s << " " << order_by_;
        }

        if (trace_flag_4199_)
        {
            s << " OPTION(QUERYTRACEON 4199)";
        }
        
        shared_ptr<IResultSet> rs = get_resultset(conn, s.str(), pool, proc_ctx);
        return std::make_shared<mssql_featureset>(rs, ctx,  wkb_, geometryColumnType_ == "geography",  !key_field_.empty(), key_field_as_attribute_);

    }

    return featureset_ptr();
}


featureset_ptr mssql_datasource::features_at_point(coord2d const& pt, double tol) const
{
#ifdef MAPNIK_STATS
    mapnik::progress_timer __stats__(std::clog, "mssql_datasource::features_at_point");
#endif
    CnxPool_ptr pool = ConnectionManager::instance().getPool(creator_.id());
    if (pool)
    {
        shared_ptr<Connection> conn = pool->borrowObject();
        if (!conn)
        {
            return featureset_ptr();
        }

        if (conn->isOK())
        {
            if (geometryColumn_.empty())
            {
                std::ostringstream s_error;
                s_error << "MSSQL: geometry name lookup failed for table '";

                if (!schema_.empty())
                {
                    s_error << schema_ << ".";
                }
                s_error << geometry_table_
                        << "'. Please manually provide the 'geometry_field' parameter or add an entry "
                        << "in the geometry_columns for '";

                if (!schema_.empty())
                {
                    s_error << schema_ << ".";
                }
                s_error << geometry_table_ << "'.";

                throw mapnik::datasource_exception(s_error.str());
            }

            std::ostringstream s;
            s << "SELECT ";

            if (row_limit_ > 0)
            {
                s << " TOP " << row_limit_;
            }
            s << "[" << geometryColumn_ << "]";
            if (wkb_)
            {
                s << ".STAsBinary()";
            }
            s << " AS geom";
            mapnik::context_ptr ctx = std::make_shared<mapnik::context_type>();
            auto const& desc = desc_.get_descriptors();

            if (!key_field_.empty())
            {
                mapnik::sql_utils::quote_attr(s, key_field_);
                if (key_field_as_attribute_)
                {
                    ctx->push(key_field_);
                }
                
                for (auto const& attr_info : desc)
                {
                    std::string const& name = attr_info.get_name();
                    if (name != key_field_)
                    {
                        mapnik::sql_utils::quote_attr(s, name);
                        ctx->push(name);
                    }
                }
            }
            else
            {
                for (auto const& attr_info : desc)
                {
                    std::string const& name = attr_info.get_name();
                    mapnik::sql_utils::quote_attr(s, name);
                    ctx->push(name);
                }
            }

            box2d<double> box(pt.x - tol, pt.y - tol, pt.x + tol, pt.y + tol);
            std::string table_with_bbox = populate_tokens(table_, FMAX, box, 0, 0, mapnik::attributes());

            s << " FROM " << table_with_bbox;

            if (trace_flag_4199_)
            {
                s << " OPTION(QUERYTRACEON 4199)";
            }
            shared_ptr<IResultSet> rs = get_resultset(conn, s.str(), pool);
            return std::make_shared<mssql_featureset>(rs, ctx, wkb_, geometryColumnType_ == "geography", !key_field_.empty(), key_field_as_attribute_);
        }
    }

    return featureset_ptr();
}

box2d<double> mssql_datasource::envelope() const
{
    if (extent_initialized_)
    {
        return extent_;
    }

    CnxPool_ptr pool = ConnectionManager::instance().getPool(creator_.id());
    if (pool)
    {
        shared_ptr<Connection> conn = pool->borrowObject();
        if (!conn)
        {
            return extent_;
        }
        if (conn->isOK())
        {
            std::ostringstream s;

            if (geometryColumn_.empty())
            {
                std::ostringstream s_error;
                s_error << "MSSQL: unable to query the layer extent of table '";

                if (!schema_.empty())
                {
                    s_error << schema_ << ".";
                }
                s_error << geometry_table_ << "' because we cannot determine the geometry field name."
                        << "\nPlease provide either an 'extent' parameter to skip this query, "
                        << "a 'geometry_field' and/or 'geometry_table' parameter, or add a "
                        << "record to the 'geometry_columns' for your table.";

                throw mapnik::datasource_exception("Mssql Plugin: " + s_error.str());
            }

            if (estimate_extent_)
            {
                if (geometryColumnType_ == "geometry")
                {
                    s << "SELECT ext.STPointN(1).STX AS MinX, ext.STPointN(1).STY AS MinY,ext.STPointN(3).STX AS MaxX, ext.STPointN(3).STY AS MaxY"
                      << " FROM (SELECT " << geometryColumnType_ << "::EnvelopeAggregate(" << geometryColumn_ << ") as ext from ";
                }
                else
                {
                    s << "SELECT ext.STPointN(1).Long AS MinX, ext.STPointN(1).Lat AS MinY,ext.STPointN(3).Long AS MaxX, ext.STPointN(3).Lat AS MaxY "
                      << " FROM (SELECT " << geometryColumnType_ << "::EnvelopeAggregate(" << geometryColumn_ << ") as ext from ";
                }
                if (!schema_.empty())
                {
                    s << schema_ << ".";
                }
                s << geometry_table_ << " TABLESAMPLE (25 percent) ) as tmp";
            }
            else
            {
                if (geometryColumnType_ == "geometry")
                {
                    s << "SELECT ext.STPointN(1).STX AS MinX, ext.STPointN(1).STY AS MinY,ext.STPointN(3).STX AS MaxX, ext.STPointN(3).STY AS MaxY"
                      << " FROM (SELECT " << geometryColumnType_ << "::EnvelopeAggregate(" << geometryColumn_ << ") as ext from ";
                }
                else
                {
                    s << "SELECT ext.STPointN(1).Long AS MinX, ext.STPointN(1).Lat AS MinY,ext.STPointN(3).Long AS MaxX, ext.STPointN(3).Lat AS MaxY "
                      << " FROM (SELECT " << geometryColumnType_ << "::EnvelopeAggregate(" << geometryColumn_ << ") as ext from ";
                }

                if (extent_from_subquery_)
                {
                    // if a subselect limits records then calculating the extent upon the
                    // subquery will be faster and the bounds will be more accurate
                    s << populate_tokens(table_) << ") as tmp";
                }
                else
                {
                    if (!schema_.empty())
                    {
                        s << schema_ << ".";
                    }

                    // but if the subquery does not limit records then querying the
                    // actual table will be faster as indexes can be used
                    s << geometry_table_ << ") as tmp";
                }
            }

            shared_ptr<ResultSet> rs = conn->executeQuery(s.str());
            if (rs->next() && !rs->isNull(0))
            {
                double lox, loy, hix, hiy;
                if ((lox = *rs->getDouble(0)) &&
                        (loy = *rs->getDouble(1)) &&
                        (hix = *rs->getDouble(2)) &&
                        (hiy = *rs->getDouble(3)))
                {
                    extent_.init(lox, loy, hix, hiy);
                    extent_initialized_ = true;
                }
                else
                {
                    MAPNIK_LOG_DEBUG(mssql) << "mssql_datasource: Could not determine extent from query: " << s.str();
                }
            }
            rs->close();
        }
    }

    return extent_;
}

boost::optional<mapnik::datasource_geometry_t> mssql_datasource::get_geometry_type() const
{
    //return boost::optional<mapnik::datasource::geometry_t>();

    boost::optional<mapnik::datasource_geometry_t> result;

    CnxPool_ptr pool = ConnectionManager::instance().getPool(creator_.id());
    if (pool)
    {
        shared_ptr<Connection> conn = pool->borrowObject();
        if (!conn)
        {
            return result;
        }
        if (conn->isOK())
        {
            std::ostringstream s;
            std::string g_type;

            if (g_type.empty() && !geometryColumn_.empty())
            {
                s.str("");

                std::string prev_type("");

                s << "SELECT ";
                if (row_limit_ > 0 && row_limit_ < 5)
                {
                    s << " TOP " << row_limit_;
                }
                else
                {
                    s << " TOP 5";
                }

                s << "[" << geometryColumn_ << "].STGeometryType() AS geom"
                  << " FROM " << populate_tokens(table_);


                shared_ptr<ResultSet> rs = conn->executeQuery(s.str());
                while (rs->next() && !rs->isNull(0))
                {
                    std::string data = rs->getString(0);

                    if (boost::algorithm::contains(data, "line"))
                    {
                        g_type = "linestring";
                        result.reset(mapnik::datasource_geometry_t::LineString);
                    }
                    else if (boost::algorithm::contains(data, "point"))
                    {
                        g_type = "point";
                        result.reset(mapnik::datasource_geometry_t::Point);
                    }
                    else if (boost::algorithm::contains(data, "polygon"))
                    {
                        g_type = "polygon";
                        result.reset(mapnik::datasource_geometry_t::Polygon);
                    }
                    else // geometry
                    {
                        result.reset(mapnik::datasource_geometry_t::Collection);
                        return result;
                    }
                    if (!prev_type.empty() && g_type != prev_type)
                    {
                        result.reset(mapnik::datasource_geometry_t::Collection);
                        return result;
                    }
                    prev_type = g_type;
                }
            }
        }
    }

    return result;
}
