#pragma once

#ifndef _META_H
#define _META_H 0

#include<vector>
#include <string>
#include "Utilities.h"
#include "DbString.h"


template <typename dt>
struct data{
    unsigned int row_id;
    bool is_deleted;
    dt datum;
};


template <typename dt>
struct block_meta
{
    int count;
    dt min;
    dt max; 

    block_meta(){
        count = 0;
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


template<typename T>
void recalculate_meta(block_meta<T>& meta, const std::vector<data<T>>& block_data);

template<typename T>
void block_meta_recalculation(std::vector<block_obj<T>> * data_chunk);

template <typename T>
std::pair<block_meta<T> *, int> get_all_block_meta(const std::string &table_name, const std::string &col_name);


#endif