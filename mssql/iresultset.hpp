#pragma once


class IResultSet
{
public:
    //virtual IResultSet& operator=(const IResultSet& rhs) = 0;
    virtual ~IResultSet() {}
    virtual void close() = 0;
    virtual int getNumFields() const = 0;
    virtual bool next() = 0;
    virtual const std::string getFieldName(int index) const = 0;
    virtual int getFieldLength(int index) const = 0;
    virtual int getFieldLength(const char* name) const = 0;
    virtual int getTypeOID(int index) const = 0;
    virtual int getTypeOID(const char* name) const = 0;
    virtual bool isNull(int index) const = 0;
    virtual const int getInt(int index) const = 0;
    virtual const double getDouble(int index) const = 0;
    virtual const float getFloat(int index) const = 0;
    virtual const std::string getString(int index) const = 0;
    virtual const std::wstring getWString(int index) const = 0;
    virtual const std::vector<char> getBinary(int index) const = 0;

};