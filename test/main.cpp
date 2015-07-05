#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

//mapnik
#include <mapnik/datasource.hpp>
#include <mapnik/datasource_cache.hpp>

static std::string MSSQL_CONNECTION_STRING = (std::getenv("MSSQL_CONNECTION_STRING") == nullptr) ?
    "Driver={SQL Server Native Client 11.0};Server=.;Database=mapnik_tmp_mssql_db;Trusted_Connection=Yes;" :
    std::getenv("MSSQL_CONNECTION_STRING");

TEST_CASE("mssql") {

    //register plugin
    std::string mssql_plugin = "mssql.input";
    mapnik::datasource_cache::instance().register_datasource(mssql_plugin);

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
        p["srid"] = 900913;
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
            REQUIRE(5 == poly.exterior_ring.size());
            REQUIRE(0 == poly.exterior_ring[0].x);
            REQUIRE(0 == poly.exterior_ring[0].y);

            //bigint
            auto _bigint = f1->get("_bigint");
            mapnik::value_integer _bigint_value = _bigint.to_int();
            REQUIRE(2147483648 == _bigint_value);

            //int
            auto _int = f1->get("_int");
            mapnik::value_integer int_value = _int.to_int();
            REQUIRE(1 == int_value);
           
            //nvarchar
            mapnik::value_unicode_string nvarchar_string = f1->get("_nvarchar").to_unicode();
            REQUIRE(0 == nvarchar_string.compare(UnicodeString::fromUTF8(StringPiece("\x61\x62\xC2\xA9\xC4\x8E\xC3\xA9\xE2\x92\xBB\xE2\x98\x80"))));
            
            //next feature should be empty
            mapnik::feature_ptr f2 = fs->next();
            REQUIRE(mapnik::feature_ptr() == f2);
        }
    }
}

int main (int argc, char* const argv[])
{
    int result = Catch::Session().run( argc, argv );
    return result;
}
