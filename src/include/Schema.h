#pragma once

#ifndef _SCHEMA_H
#define _SCHEMA_H 0

#include <iostream>
#include <vector>
#include <cstring>
#include "Utilities.h"

struct schema_meta{
    int number_of_columns;
    int* data_type;
    std::pair <int,char *> *fields;
};

schema_meta* read_schema(std::string table_name);
void write_schema(std::string table_name,std::vector <std::string> &columns_name, std::vector <int> &columns_data_type);

#endif