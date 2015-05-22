#pragma once

#include <cstdint>
#include <vector>

enum FIGURE : uint8_t {
	FIGURE_INTERIOR_RING = 0x00,
	FIGURE_STROKE = 0x01,
	FIGURE_EXTERIOR_RING = 0x02,
	FIGURE_V2_POINT = 0x00,
	FIGURE_V2_LINE = 0x01,
	FIGURE_V2_ARC = 0x02,
	FIGURE_V2_COMPOSITE_CURVE = 0x03
};

enum SHAPE : uint8_t {
	SHAPE_POINT = 0x01,
	SHAPE_LINESTRING = 0x02,
	SHAPE_POLYGON = 0x03,
	SHAPE_MULTIPOINT = 0x04,
	SHAPE_MULTILINESTRING = 0x05,
	SHAPE_MULTIPOLYGON = 0x06,
	SHAPE_GEOMETRY_COLLECTION = 0x07,
	//V2
	SHAPE_CIRCULAR_STRING = 0x08,
	SHAPE_COMPOUND_CURVE = 0x09,
	SHAPE_CURVE_POLYGON = 0x0A,
};

enum SEGMENT : uint8_t {
	SEGMENT_LINE = 0x00,
	SEGMENT_ARC = 0x01,
	SEGMENT_FIRST_LINE = 0x02,
	SEGMENT_FIRST_ARC = 0x03,
};

struct Figure {
	Figure() {}
	Figure(FIGURE f_, uint32_t o_)
		: Attribute(f_), Offset(o_)
	{}
	FIGURE Attribute;
	uint32_t Offset;
};

struct Shape {
	Shape() {}
	Shape(int32_t parent_offset_, int32_t figure_offset_, SHAPE shape_)
		: ParentOffset(parent_offset_),
		FigureOffset(figure_offset_),
		OpenGisType(shape_)
	{}
	int32_t ParentOffset;
	int32_t FigureOffset;
	SHAPE OpenGisType;
	int index;
};

struct Point {
	Point() {}
	Point(double x_, double y_)
		: X(x_),
		Y(y_)
	{}
	double X;
	double Y;
	double Z;
	double M;
};
struct Segment {
	SEGMENT Type;
};

struct SerializationProperties {
	bool H;  //V2
	bool L;
	bool P;
	bool V;
	bool M;
	bool Z;
};

struct Geometry {
	int32_t SRID;
	uint8_t Version;
	SerializationProperties Properties;

	std::vector<Point> Points;
	std::vector<Figure> Figures;
	std::vector<Shape> Shapes;
	std::vector<Segment> Segments;

	const std::vector<Figure> getFiguresFromShape(int shapeIdx) const {
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
	const std::vector<Point> getPointsFromFigure(int figureIdx) const {
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
};

class udt_reader
{
private:
	const char* data_;
	std::size_t size_;
	std::size_t pos_;


private:
	uint32_t read_uint32()
	{
		std::uint32_t val;

		auto data = data_ + pos_;
		std::memcpy(&val, data, 4);
		pos_ += 4;

		return val;
	}
	
	uint16_t read_uint16()
	{
		std::uint16_t val;

		auto data = data_ + pos_;
		std::memcpy(&val, data, 2);
		pos_ += 2;

		return val;
	}
	uint8_t read_uint8()
	{
		std::uint8_t val;

		auto data = data_ + pos_;
		std::memcpy(&val, data, 1);
		pos_ += 1;

		return val;
	}
	double read_double()
	{
		double val;

		auto data = data_ + pos_;
		std::memcpy(&val, &data[0], 8);
		pos_ += 8;

		return val;
	}

	void readPointsZ(std::vector<Point> points) {

		if (points.empty()) {
			return;
		}
		for (auto point : points)
		{
			point.Z = read_double();
		}

	}

	void readPointsM(std::vector<Point> points) {

		if (points.empty()) {
			return;
		}
		for (auto point : points)
		{
			point.M = read_double();
		}

	}
	std::vector<Point> readPoints(uint32_t count) {
		std::vector<Point> points;

		if (count < 1) {
			return std::vector<Point>();
		}

		for (uint32_t i = 0; i < count; i++) {
						
			double x = read_double();
			double y = read_double();
			points.emplace_back(x, y);

		}

		return points;
	}

	std::vector<Figure> readFigures(uint32_t count, SerializationProperties properties) {
		std::vector<Figure> figures;
		if (count == 0) {
			return figures;
		}

		if (properties.P || properties.L) {
			figures.emplace_back(FIGURE_STROKE, 0);
		}
		else {
			for (int i = 0; i < count; i++) {

				Figure f;
				f.Attribute = (FIGURE)read_uint8();
				f.Offset = read_uint32();				
				figures.push_back(f);
			}
		}
		return figures;
	}
	std::vector<Shape> readShapes(uint32_t count, SerializationProperties properties) {
		std::vector<Shape> shapes;
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
	std::vector<Segment> readSegments(uint32_t count) {
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

public:
	udt_reader(const char* data, std::size_t size)
		: data_(data),
		size_(size),
		pos_(0)
	{
	}
	Geometry parseGeometry(bool isGeography) {
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
				throw std::exception("Invalid SRID for geography");
			}
		}

		//version
		g.Version = read_uint8();

		if (g.Version > 2) {
			throw std::exception("Version %d is not supported", g.Version);
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
			throw std::exception("geometry data is invalid");
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
		g.Points = readPoints(numberOfPoints);

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
public:
};

