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
#include <iostream>
#include <limits>


struct Dbstr{
    int file;
    int offset_start;
    int size;
    bool operator>(const Dbstr& other) const {
        return false;
    }
    bool operator<(const Dbstr& other) const {
        return false;
    }
    bool operator==(const Dbstr& other) const {
        return false;
    }
};




template <typename dt>
struct data{
    unsigned int row_id;
    bool is_deleated;
    dt data;  //Rename data as dadum and add null bit and constructor
};




template <typename dt>
struct block_meta
{
    int count;
    dt min;
    dt max; 
    dt sum;

    block_meta(){
        count = 0;
        sum = dt(); 
        if (typeid(dt) != typeid(Dbstr))
        {
            min = std::numeric_limits<dt>::max();
            max = std::numeric_limits<dt>::lowest();
        }
        
    }

};

template <typename T>
struct block_obj
{
    block_meta<T> meta;

    std::vector<data<T>> all_data;

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
    std::pair <int,char *> *fields;
};


#endif