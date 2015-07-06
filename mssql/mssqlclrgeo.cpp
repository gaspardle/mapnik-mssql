#include "mssqlclrgeo.hpp"
#include <cstring>

namespace mssqlclr
{


	const std::vector<Figure> Geometry::getFiguresFromShape(int shapeIdx) const {
		std::vector<Figure> figures_list;
		if (Figures.empty()) {
			return figures_list;
		}
		int32_t offset = Shapes[shapeIdx].FigureOffset;

		if (Shapes.size() > shapeIdx + 1) {
			int32_t offsetEnd = Shapes[shapeIdx + 1].FigureOffset;

			figures_list = std::vector<Figure>(Figures.cbegin() + offset, Figures.cbegin() + offsetEnd);
		}
		else {
			figures_list = std::vector<Figure>(Figures.cbegin() + offset, Figures.cend());
		}
		return figures_list;
	}

	const std::vector<Point> Geometry::getPointsFromFigure(int figureIdx) const {
		std::vector<Point> points_list;

		if (Points.empty()) {
			return points_list;
		}

		int32_t offset = Figures[figureIdx].Offset;

		if (Figures.size() > figureIdx + 1) {
			int32_t offsetEnd = Figures[figureIdx + 1].Offset;
			points_list = std::vector<Point>(Points.cbegin() + offset, Points.cbegin() + offsetEnd);
		}
		else {
			points_list = std::vector<Point>(Points.cbegin() + offset, Points.cend());
		}
		return points_list;
	}

	
	sqlgeo_reader::sqlgeo_reader(const char* data, std::size_t size)
		: data_(data),
		size_(size),
		pos_(0)
	{
	}

	uint32_t sqlgeo_reader::read_uint32()
	{
		std::uint32_t val;
		if (pos_ + 4 > size_) {
			return 0;
		}
		auto data = data_ + pos_;
		std::memcpy(&val, data, 4);
		pos_ += 4;

		return val;
	}

	uint16_t sqlgeo_reader::read_uint16()
	{
		std::uint16_t val;
		if (pos_ + 2 > size_) {
			return 0;
		}
		auto data = data_ + pos_;
		std::memcpy(&val, data, 2);
		pos_ += 2;

		return val;
	}
	uint8_t sqlgeo_reader::read_uint8()
	{
		std::uint8_t val;
		if (pos_ + 1 > size_) {
			return 0;
		}
		auto data = data_ + pos_;
		std::memcpy(&val, data, 1);
		pos_ += 1;

		return val;
	}
	double sqlgeo_reader::read_double()
	{
		double val;
		if (pos_ + 8 > size_) {
			return 0;
		}
		auto data = data_ + pos_;
		std::memcpy(&val, &data[0], 8);
		pos_ += 8;

		return val;
	}

	void sqlgeo_reader::readPointsZ(std::vector<Point> points) {

		if (points.empty()) {
			return;
		}
		for (auto& point : points)
		{
			point.Z = read_double();
		}

	}

	void sqlgeo_reader::readPointsM(std::vector<Point> points) {

		if (points.empty()) {
			return;
		}
		for (auto& point : points)
		{
			point.M = read_double();
		}

	}
	std::vector<Point> sqlgeo_reader::readPoints(uint32_t count, bool isGeography) {
		std::vector<Point> points;
		points.reserve(count);

		if (count < 1) {
			return std::vector<Point>();
		}

		for (uint32_t i = 0; i < count; i++) {
			if (isGeography) {
				double y = read_double();
				double x = read_double();
				points.emplace_back(x, y);
			}
			else {
				double x = read_double();
				double y = read_double();
				points.emplace_back(x, y);
			}

		}

		return points;
	}

	std::vector<Figure> sqlgeo_reader::readFigures(uint32_t count, SerializationProperties properties) {
		std::vector<Figure> figures;
		figures.reserve(count);

		if (count == 0) {
			return figures;
		}

		if (properties.P || properties.L) {
			figures.emplace_back(FIGURE_STROKE, 0);
		}
		else {
			for (uint32_t i = 0; i < count; i++) {

				Figure f;
				f.Attribute = (FIGURE)read_uint8();
				f.Offset = read_uint32();
				figures.push_back(f);
			}
		}
		return figures;
	}
	std::vector<Shape> sqlgeo_reader::readShapes(uint32_t count, SerializationProperties properties) {
		std::vector<Shape> shapes;
		shapes.reserve(count);

		if (count < 1) {
			return shapes;
		}

		if (properties.P) {
			shapes.emplace_back(-1, 0, SHAPE_POINT);
		}
		else if (properties.L) {
			shapes.emplace_back(-1, 0, SHAPE_LINESTRING);
		}
		else {
			for (int i = 0; i < int(count); i++) {
				uint32_t parent_offset = read_uint32();
				uint32_t figure_offset = read_uint32();
				SHAPE type = (SHAPE)read_uint8();
				shapes.emplace_back(parent_offset, figure_offset, type);
			}
		}
		return shapes;
	}
	std::vector<Segment> sqlgeo_reader::readSegments(uint32_t count) {
		std::vector<Segment> segments;
		if (count < 1) {
			return segments;
		}

		for (int i = 0; i < int(count); i++) {
			Segment s;
			s.Type = (SEGMENT)read_uint8();
			segments.push_back(s);
		}
		return segments;
	}

	Geometry sqlgeo_reader::parseGeography() {
		return parseGeometry(true);
	}

	Geometry sqlgeo_reader::parseGeometry(bool isGeography) {

		Geometry g = {};

		uint32_t numberOfPoints;
		uint32_t numberOfFigures;
		uint32_t numberOfShapes;
		uint32_t numberOfSegments;

		g.SRID = read_uint32();


		if (isGeography == true) {
			if (g.SRID == -1) {
				return g;
			}
			else if (g.SRID < 4210 || g.SRID > 4999) {
				g.Properties.V = false;
				return g;
				//("Invalid SRID for geography");
			}
		}

		//version
		g.Version = read_uint8();

		if (g.Version > 2) {
			g.Properties.V = false;
			return g;
			//("Version %d is not supported", g.Version);
		}

		//flags
		uint8_t flags = 0;
		flags = read_uint8();

		g.Properties.Z = (flags & (1 << 0)) != 0;
		g.Properties.M = (flags & (1 << 1)) != 0;
		g.Properties.V = (flags & (1 << 2)) != 0;
		g.Properties.P = (flags & (1 << 3)) != 0;
		g.Properties.L = (flags & (1 << 4)) != 0;

		if (g.Version == 2) {
			g.Properties.H = (flags & (1 << 5)) != 0;
		}
		if (g.Properties.P && g.Properties.L) {
			g.Properties.V = false;
			return g;
			//("geometry data is invalid");
		}

		//points
		if (g.Properties.P) {
			numberOfPoints = 1;
		}
		else if (g.Properties.L) {
			numberOfPoints = 2;
		}
		else {
			numberOfPoints = read_uint32();
		}
		g.Points = readPoints(numberOfPoints, isGeography);

		if (g.Properties.Z) {
			readPointsZ(g.Points);
		}
		if (g.Properties.M) {
			readPointsM(g.Points);
		}

		//figures
		if (g.Properties.P || g.Properties.L) {
			numberOfFigures = 1;
		}
		else {
			numberOfFigures = read_uint32();
		}

		g.Figures = readFigures(numberOfFigures, g.Properties);


		//shapes
		if (g.Properties.P || g.Properties.L) {
			numberOfShapes = 1;
		}
		else {
			numberOfShapes = read_uint32();
		}
		g.Shapes = readShapes(numberOfShapes, g.Properties);

		//segments
		if (g.Version == 2) {
			numberOfSegments = read_uint32();
			g.Segments = readSegments(numberOfSegments);
		}
		return g;
	}
}