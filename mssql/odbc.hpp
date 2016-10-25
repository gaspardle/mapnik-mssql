#ifndef ODBC_HPP
#define ODBC_HPP

#include <sqlext.h>
#include <string>
#include <memory>

class Odbc
{
   

public:
    Odbc();
    ~Odbc();
    SQLHANDLE getEnvHandle();
    static std::shared_ptr<Odbc> getInstance();   
  
private: 
    SQLHANDLE sqlenvhandle_;

};

std::string getOdbcError(unsigned int handletype, const SQLHANDLE& handle);

#endif // ODBC_HPP