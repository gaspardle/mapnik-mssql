#ifndef ODBC_HPP
#define ODBC_HPP

#include <string>
#include <sqlext.h>

std::string getOdbcError(unsigned int handletype, const SQLHANDLE& handle);

#endif // ODBC_HPP