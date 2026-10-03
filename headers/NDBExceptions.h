//
// Created by riju on 9/4/26.
//

#ifndef NEBULADB_NDBEXCEPTIONS_H
#define NEBULADB_NDBEXCEPTIONS_H
#include <stdexcept>


class IndexNotFoundException : public std::runtime_error {
public:
    explicit IndexNotFoundException(const std::string& msg)
        : std::runtime_error(msg) {}
};

class PageNotFoundException : public std::runtime_error {
public:
    explicit PageNotFoundException(const std::string& msg)
        : std::runtime_error(msg) {}
};



#endif //NEBULADB_NDBEXCEPTIONS_H