
#ifdef _WINDOWS
#define NOMINMAX
#include <windows.h>
#endif

#include <stdexcept>
#include "odbc.hpp"

void Odbc::InitOdbc() {
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

void Odbc::FreeOdbc() {
    SQLFreeHandle(SQL_HANDLE_DBC, sqlenvhandle_);
}

SQLHANDLE  Odbc::GetEnvHandle() {
    return Odbc::sqlenvhandle_;
}

SQLHANDLE Odbc::sqlenvhandle_;

std::string getOdbcError(unsigned int handletype, const SQLHANDLE& handle)
{

    std::string  status;

    SQLCHAR  sqlstate[6];
    SQLCHAR  message[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER  NativeError;
    SQLSMALLINT   i, MsgLen;
    SQLRETURN     rc2;

    // Get the status records.
    i = 1;
    while ((rc2 = SQLGetDiagRecA(handletype, handle, i, sqlstate, &NativeError,
                                 message, sizeof(message), &MsgLen)) != SQL_NO_DATA)
    {
        status += "(" + std::to_string((short)i) + ")";
        status += "\nSQLState: ";
        status += ((char*)&sqlstate[0]);
        status += "\nNativeError: " + std::to_string((long)NativeError);
        status += "\nMessage: ";
        status += (char*)&message[0];
        status += "\nMsgLen: " + std::to_string((long)MsgLen);

        i++;
    }

    return status;
}