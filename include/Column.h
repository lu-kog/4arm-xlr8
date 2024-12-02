#pragma once

#ifndef _COLUMN_H
#define _COLUMN_H 0

#include <vector>
#include "Schema.h"
#include "DbString.h"
#include "Utilities.h"
#include "Meta.h"

struct column_meta
{
    int total_records;
    int data_type;
    int no_block;
};


struct column_obj
{
    column_meta meta;

    union data_union{
        std::vector<data<int>> *int_data;
        std::vector<data<float>> *float_data;
        std::vector<data<long>> *long_data;
        std::vector<data<double>> *double_data;
        std::vector<data<Dbstr>> *str_data;
        std::vector<data<char>> *char_data;
    } all_data;


};

void create_vec(column_obj &column);
void delete_vec(column_obj &column);
void write_column_meta(const std::string &table_name, const std::string &column_name , column_meta &col_meta_to_write);
column_meta * get_column_meta(const std::string &table_name, const std::string &column_name);
std::vector<std::string> * get_all_column_names(std::string table_name);

void initilaize_column_objs(const schema_meta &schema, std::vector<column_obj> &table_data,const int &total_data);


#endif