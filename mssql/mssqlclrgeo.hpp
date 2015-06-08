#ifndef MSSQL_MSSQLCLRGEO_HPP
#define MSSQL_MSSQLCLRGEO_HPP

#include <cstdint>
#include <vector>

namespace mssqlclr
{

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
	const std::vector<Figure> getFiguresFromShape(int shapeIdx) const;
	const std::vector<Point> getPointsFromFigure(int figureIdx) const;
};

class sqlgeo_reader
{
private:
	uint32_t read_uint32();
	uint16_t read_uint16();
	uint8_t read_uint8();
	double read_double();
	void readPointsZ(std::vector<Point> points);
	void readPointsM(std::vector<Point> points);
	std::vector<Point> readPoints(uint32_t count, bool isGeography);
	std::vector<Figure> readFigures(uint32_t count, SerializationProperties properties);
	std::vector<Shape> readShapes(uint32_t count, SerializationProperties properties);
	std::vector<Segment> readSegments(uint32_t count);

	const char* data_;
	std::size_t size_;
	std::size_t pos_;

public:

	sqlgeo_reader(const char* data, std::size_t size);
    Geometry parseGeometry(bool isGeography = false);
    Geometry parseGeography();

};

class sqlgeo_writer {
};

}

#endif //MSSQL_MSSQLCLRGEO_HPP