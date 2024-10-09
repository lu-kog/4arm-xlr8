#ifndef _cred_h
#define _cred_h 1
#include <vector>
#include <string>
#include "meta.h"
void create_table(std::string table_name,std::vector <std::string> &columns_name, std::vector <int> &columns_data_type);
void get_home_folder();
schema_meta *read_schema(std::string table_name);


#endif