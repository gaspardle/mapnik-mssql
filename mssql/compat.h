#ifndef MSSQL_COMPAT_HPP
#define MSSQL_COMPAT_HPP

#include <mapnik/version.hpp>


#if MAPNIK_VERSION <= 200300

#define SHARED_PTR_NAMESPACE boost
#define TUPLE_NAMESPACE boost
#define BOOLEAN_TYPE mapnik::boolean
#else

#define SHARED_PTR_NAMESPACE std
#define TUPLE_NAMESPACE std
#define BOOLEAN_TYPE mapnik::boolean_type

#endif


	//using SHARED_PTR_NAMESPACE::shared_ptr;
	//using SHARED_PTR_NAMESPACE::make_shared;


#endif // MSSQL_COMPAT_HPP