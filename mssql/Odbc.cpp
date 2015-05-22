
#ifdef _WINDOWS
#define NOMINMAX
#include <windows.h>
#endif

#include "odbc.hpp"


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
		message, sizeof(message), &MsgLen)) != SQL_NO_DATA) {
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