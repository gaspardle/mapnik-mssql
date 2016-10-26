#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

//mapnik
#include <mapnik/datasource.hpp>
#include <mapnik/datasource_cache.hpp>

static std::string MSSQL_CONNECTION_STRING = (std::getenv("MSSQL_CONNECTION_STRING") == nullptr) ?
    "Driver={SQL Server Native Client 11.0};Server=.;Database=mapnik_tmp_mssql_db;Trusted_Connection=Yes;" :
    std::getenv("MSSQL_CONNECTION_STRING");

static std::string MSSQL_PLUGIN_PATH = (std::getenv("MSSQL_PLUGIN_PATH") == nullptr) ?
	"mssql.input" :
	std::getenv("MSSQL_PLUGIN_PATH");

TEST_CASE("mssql-datatypes") {

    SECTION("is registered")
    {
        std::vector<std::string> plugins = mapnik::datasource_cache::instance().plugin_names();
        std::string plugin_name = "mssql";
        auto itr = std::find(plugins.begin(), plugins.end(), plugin_name);
        REQUIRE(itr != plugins.end());
    }

    SECTION("create datasource")
    {
        mapnik::parameters p;
        p["type"] = "mssql";
        p["connection_string"] = MSSQL_CONNECTION_STRING;
        p["table"] = "table1";
        p["geometry_field"] = "geom";
        p["srid"] = "900913";
        p["extent"] = "-20037508.34,-20037508.34,20037508.34,20037508.34";
        std::shared_ptr<mapnik::datasource> ds = mapnik::datasource_cache::instance().create(p);
        auto expected_type = mapnik::datasource::datasource_t::Vector;
        auto expected_extent = mapnik::box2d<double>(-20037508.34, -20037508.34, 20037508.34, 20037508.34);
        REQUIRE(expected_type == ds->type());
        REQUIRE(expected_extent == ds->envelope());

        SECTION("query features")
        {
            mapnik::layer_descriptor ld = ds->get_descriptor();
            std::vector<mapnik::attribute_descriptor> attributes = ld.get_descriptors();
            mapnik::query q(ds->envelope());
            for (auto descriptor : attributes)
            {
                q.add_property_name(descriptor.get_name());
            }
            mapnik::featureset_ptr fs = ds->features(q);
            mapnik::feature_ptr f1 = fs->next();

            //test geom
            mapnik::geometry::geometry<double> geom = f1->get_geometry();
            REQUIRE(geom.is<mapnik::geometry::polygon<double>>());
            auto const& poly = mapnik::util::get<mapnik::geometry::polygon<double>>(geom);

            REQUIRE(1 == poly.num_rings());
            REQUIRE(5 == poly.exterior_ring.size());
            REQUIRE(0 == poly.exterior_ring[0].x);
            REQUIRE(0 == poly.exterior_ring[0].y);
            REQUIRE(1 == poly.exterior_ring[1].x);
            REQUIRE(0 == poly.exterior_ring[1].y);
            REQUIRE(1 == poly.exterior_ring[2].x);
            REQUIRE(1 == poly.exterior_ring[2].y);
            REQUIRE(0 == poly.exterior_ring[3].x);
            REQUIRE(1 == poly.exterior_ring[3].y);
            REQUIRE(0 == poly.exterior_ring[4].x);
            REQUIRE(0 == poly.exterior_ring[4].y);
            
            //bigint
            auto _bigint = f1->get("_bigint");
            mapnik::value_integer _bigint_value = _bigint.to_int();
            REQUIRE(2147483648 == _bigint_value);

            //int
            auto _int = f1->get("_int");
            mapnik::value_integer int_value = _int.to_int();
            REQUIRE(1 == int_value);

            //float
            auto _float = f1->get("_float");
            mapnik::value_double float_value = _float.to_double();
            REQUIRE(1.25 == Approx(float_value));

            //real
            auto _real = f1->get("_real");
            mapnik::value_double real_value = _real.to_double();
            REQUIRE(1.25 == Approx(real_value));

            //bit
            auto _bit = f1->get("_bit");
            mapnik::value_bool bit_value = _bit.to_bool();
            REQUIRE(true == bit_value);

            //nvarchar
            mapnik::value_unicode_string nvarchar_string = f1->get("_nvarchar").to_unicode();
            REQUIRE(0 == nvarchar_string.compare(UnicodeString::fromUTF8(StringPiece("\x61\x62\xC2\xA9\xC4\x8E\xC3\xA9\xE2\x92\xBB\xE2\x98\x80"))));

            
            //numeric / decimal       
            /*
            auto _decimal = f1->get("_decimal");
            mapnik::value_double decimal_value = _decimal.to_double();
            REQUIRE(1.25 == Approx(decimal_value));

            auto _numeric = f1->get("_numeric");
            mapnik::value_double numeric_value = _numeric.to_double();
            REQUIRE(1.25 == Approx(numeric_value));
            */

            //money
            auto _money = f1->get("_money");
            mapnik::value_double money_value = _money.to_double();
            REQUIRE(1.25 == Approx(money_value));

            //next feature should be empty
            mapnik::feature_ptr f2 = fs->next();
            REQUIRE(mapnik::feature_ptr() == f2);
        }
    }
}

int main (int argc, char* argv[])
{
    //register plugin
    mapnik::datasource_cache::instance().register_datasource(MSSQL_PLUGIN_PATH);
    
    int result = Catch::Session().run( argc, argv );
    return result;
}
