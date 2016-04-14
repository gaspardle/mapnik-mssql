#ifndef MSSQL_GEOCLR_READER_HPP
#define MSSQL_GEOCLR_READER_HPP

#include <mapnik/geometry.hpp>
#include <mapnik/geometry_correct.hpp>
#include <mapnik/util/noncopyable.hpp>

#include <cstdint>
#include <vector>

#include "mssqlclrgeo.hpp"

struct mapnik_geoclr_reader : mapnik::util::noncopyable
{
  private:
    mssqlclr::sqlgeo_reader reader;
    uint32_t shape_pos_;
    mssqlclr::Geometry geo_;
    bool is_geography_;

  public:
    mapnik_geoclr_reader(const char* data, std::size_t size, bool isGeography)
        : reader(mssqlclr::sqlgeo_reader(data, size)),
          shape_pos_(0),
          is_geography_(isGeography)
    {
        geo_ = reader.parseGeometry(is_geography_);
    }

    mapnik::geometry::point<double> read_point(mssqlclr::Shape shape)
    {

        auto figures = geo_.getFiguresFromShape(shape_pos_);
        auto pt_offset = figures[0].Offset;

        double x = geo_.Points[pt_offset].X;
        double y = geo_.Points[pt_offset].Y;

        return mapnik::geometry::point<double>(x, y);
    }
    mapnik::geometry::multi_point<double> read_multipoint(mssqlclr::Shape shape)
    {

        mapnik::geometry::multi_point<double> multi_point;

        mssqlclr::Geometry parent_geo = geo_;
        uint32_t parent_pos = shape_pos_;
        while ((parent_geo.Shapes.size() - 1 >= shape_pos_ + 1) && parent_geo.Shapes[shape_pos_ + 1].ParentOffset == parent_pos)
        {
            shape_pos_++;
            multi_point.emplace_back(read_point(parent_geo.Shapes[shape_pos_]));
        }

        return multi_point;
    }

    mapnik::geometry::line_string<double> read_linestring(mssqlclr::Shape shape)
    {
        mapnik::geometry::line_string<double> line;
        auto figures = geo_.getFiguresFromShape(shape_pos_);

        if (!figures.empty())
        {
            auto points = geo_.getPointsFromFigure(shape.FigureOffset);

            size_t num_points = points.size();
            if (num_points > 0)
            {
                line.reserve(num_points);
                for (auto p : points)
                {
                    line.emplace_back(p.X, p.Y);
                }
            }
        }
        return line;
    }
    mapnik::geometry::multi_line_string<double> read_multilinestring(mssqlclr::Shape shape)
    {

        mapnik::geometry::multi_line_string<double> multi_line;

        mssqlclr::Geometry parent_geo = geo_;
        uint32_t parent_pos = shape_pos_;
        while ((parent_geo.Shapes.size() - 1 >= shape_pos_ + 1) && parent_geo.Shapes[shape_pos_ + 1].ParentOffset == parent_pos)
        {
            shape_pos_++;
            multi_line.emplace_back(read_linestring(parent_geo.Shapes[shape_pos_]));
        }

        return multi_line;
    }
    mapnik::geometry::polygon<double> read_polygon(mssqlclr::Shape shape)
    {
        auto rings = geo_.getFiguresFromShape(shape_pos_);

        size_t num_rings = rings.size();
        mapnik::geometry::polygon<double> poly;
        if (num_rings > 1)
        {
            poly.interior_rings.reserve(num_rings - 1);
        }

        for (int i = 0; i < num_rings; ++i)
        {
            mapnik::geometry::linear_ring<double> ring;
            auto points = geo_.getPointsFromFigure(shape.FigureOffset + i);
            size_t num_points = points.size();
            if (num_points > 0)
            {
                ring.reserve(num_points);
                for (const auto& p : points)
                {
                    ring.emplace_back(p.X, p.Y);
                }
            }
            if (i == 0)
            {
                poly.set_exterior_ring(std::move(ring));
            }
            else
            {
                poly.add_hole(std::move(ring));
            }
        }
        return poly;
    }
    mapnik::geometry::multi_polygon<double> read_multipolygon(mssqlclr::Shape shape)
    {

        mapnik::geometry::multi_polygon<double> multi_poly;
        mssqlclr::Geometry parent_geo = geo_;
        uint32_t parent_pos = shape_pos_;
        while ((parent_geo.Shapes.size() - 1 >= shape_pos_ + 1) && parent_geo.Shapes[shape_pos_ + 1].ParentOffset == parent_pos)
        {
            shape_pos_++;
            multi_poly.emplace_back(read_polygon(parent_geo.Shapes[shape_pos_]));
        }
        return multi_poly;
    }

    mapnik::geometry::geometry_collection<double> read_collection(mssqlclr::Shape shape)
    {
        mapnik::geometry::geometry_collection<double> collection;
        mssqlclr::Geometry parent_geo = geo_;
        uint32_t parent_pos = shape_pos_;
        while ((parent_geo.Shapes.size() - 1 >= shape_pos_ + 1) && parent_geo.Shapes[shape_pos_ + 1].ParentOffset == parent_pos)
        {
            shape_pos_++;
            collection.push_back(std::move(read()));
        }

        return collection;
    }

    mapnik::geometry::geometry<double> read()
    {
        mapnik::geometry::geometry<double> geom = mapnik::geometry::geometry_empty();

        if (geo_.Shapes.size() == 0)
        {
            return geom;
        }

        auto s = geo_.Shapes[shape_pos_];

        mssqlclr::SHAPE type = s.OpenGisType;
        switch (type)
        {
        case mssqlclr::SHAPE::SHAPE_POINT:
            geom = std::move(read_point(s));
            break;
        case mssqlclr::SHAPE::SHAPE_LINESTRING:
            geom = std::move(read_linestring(s));
            break;
        case mssqlclr::SHAPE::SHAPE_POLYGON:
            geom = std::move(read_polygon(s));
            break;
        case mssqlclr::SHAPE::SHAPE_MULTIPOINT:
            geom = std::move(read_multipoint(s));
            break;
        case mssqlclr::SHAPE::SHAPE_MULTILINESTRING:
            geom = std::move(read_multilinestring(s));
            break;
        case mssqlclr::SHAPE::SHAPE_MULTIPOLYGON:
            geom = std::move(read_multipolygon(s));
            break;
        case mssqlclr::SHAPE::SHAPE_GEOMETRY_COLLECTION:
            geom = std::move(read_collection(s));
            break;

        case mssqlclr::SHAPE::SHAPE_COMPOUND_CURVE:
        case mssqlclr::SHAPE::SHAPE_CIRCULAR_STRING:
        case mssqlclr::SHAPE::SHAPE_CURVE_POLYGON:
        default:
            //nope
            break;
        }

        return geom;
    }
};

mapnik::geometry::geometry<double> from_geoclr(const char* data,
                                               std::size_t size, bool is_geography)
{
    if (size == 0)
    {
        return mapnik::geometry::geometry_empty();
    }
    mapnik_geoclr_reader reader(data, size, is_geography);
    mapnik::geometry::geometry<double> geom(reader.read());
    // note: this will only be applied to polygons
    mapnik::geometry::correct(geom);
    return geom;
}

#endif //MSSQL_GEOCLR_READER_HPP