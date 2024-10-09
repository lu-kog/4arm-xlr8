#pragma once

#ifndef _META_H
#define _META_H 0



#define DBINT 1
#define DBCHAR 2
#define DBLONG 3
#define DBDOUBLE 4
#define DBSTRING 5
#define DBFLOAT 6

#include <string>
#include <vector>


struct Dbstr{
    int file;
    int offset_start;
    int size;
};




template <typename dt>
struct data{
    unsigned int row_id;
    dt data;
    bool is_deleated;
};




template <typename dt>
struct block_meta
{
    int count;
    dt min;
    dt max; 
    dt sum;
};

template <typename T>
struct block_obj
{
    block_meta<T> meta;

    union data_union{
        std::vector<data<int>> *int_data;
        std::vector<data<float>> *float_data;
        std::vector<data<long>> *long_data;
        std::vector<data<double>> *double_data;
        std::vector<data<Dbstr>> *str_data;
        std::vector<data<char>> *char_data;
    } all_data;

};



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



struct schema_meta{
    int number_of_columns;
    int* data_type;
    // str* fields; 
    std::pair <int,char *> *fields;
    
};


#endif