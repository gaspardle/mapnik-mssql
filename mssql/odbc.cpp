
#ifdef _WINDOWS
#define NOMINMAX
#include <windows.h>
#endif

#include "odbc.hpp"
#include <mapnik/debug.hpp>
#include <sstream>
#include <stdexcept>

Odbc::Odbc() {
    MAPNIK_LOG_DEBUG(mssql) << "mssql: InitOdbc";

    if (SQL_SUCCESS != SQLSetEnvAttr(SQL_NULL_HANDLE, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_HENV, 0))
    {
        throw std::runtime_error("Mssql Plugin: SQLSetEnvAttr SQL_ATTR_CONNECTION_POOLING failed");
    }

    if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sqlenvhandle_))
    {
        throw std::runtime_error("Mssql Plugin: SQLAllocHandle SQL_HANDLE_ENV failed");
    }

    if (SQL_SUCCESS != SQLSetEnvAttr(sqlenvhandle_, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0))
    {
        throw std::runtime_error("Mssql Plugin: SQLSetEnvAttr SQL_ATTR_ODBC_VERSION failed");
    }
}

Odbc::~Odbc() {   
    if (sqlenvhandle_ != SQL_NULL_HANDLE)
    {
        MAPNIK_LOG_DEBUG(mssql) << "mssql: SQLFreeHandle SQL_HANDLE_ENV";
        SQLFreeHandle(SQL_HANDLE_ENV, sqlenvhandle_);
        sqlenvhandle_ = SQL_NULL_HANDLE;
    }
}

std::shared_ptr<Odbc> Odbc::getInstance() {
    static std::weak_ptr<Odbc> instance;
    static std::mutex mutex;
    const std::lock_guard<std::mutex> lock(mutex);
    if (const auto result = instance.lock()) return result;
    return (instance = std::make_shared<Odbc>()).lock();
}

SQLHANDLE Odbc::getEnvHandle()
{
    return sqlenvhandle_;
}


std::string getOdbcError(unsigned int handletype, const SQLHANDLE& handle)
{
    
    std::ostringstream err;
    SQLCHAR sqlstate[6];
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER NativeError;
    SQLSMALLINT i, MsgLen;
    SQLRETURN rc2;

    // Get the status records.
    i = 1;
    while ((rc2 = SQLGetDiagRecA(handletype, handle, i, sqlstate, &NativeError,
                                 message, sizeof(message), &MsgLen)) != SQL_NO_DATA && rc2 >= 0)
    {       
        err << "(" << i << ") " << "\nSQLState:" << (char*)&sqlstate[0];
        err << "\nNativeError: " << (long)NativeError;
        err << "\nMessage: " << &message[0];
        //err << "\nMsgLen: " + (long)MsgLen;

        i++;
    }

    return err.str();
}