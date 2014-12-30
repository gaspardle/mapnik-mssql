#ifndef MSSQL_TRANSITION_HPP
#define MSSQL_TRANSITION_HPP

#include <memory>
#include <boost/make_shared.hpp>

template<typename T>
boost::shared_ptr<T> make_shared_ptr(std::shared_ptr<T>&& ptr)
{
	return boost::shared_ptr<T>(ptr.get(), [ptr](T*) mutable {ptr.reset(); });
}

template<typename T>
std::shared_ptr<T> make_shared_ptr(boost::shared_ptr<T>&& ptr)
{
	return std::shared_ptr<T>(ptr.get(), [ptr](T*) mutable {ptr.reset(); });
}

#endif // MSSQL_TRANSITION_HPP