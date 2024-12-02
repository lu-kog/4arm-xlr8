#pragma once

#ifndef _DATA_BLOCK_H
#define _DATA_BLOCK_H 0


#include <string>
#include<vector>
#include "Utilities.h"
#include "DbString.h"
#include "Meta.h"
#include "Column.h"

void createBlocks(column_obj& clm, std::string& table_name, std::string col_name);

template <typename T>
void dump_new_records(std::vector<block_obj<T>> & new_blocks, std::string &table_name, std::string &col_name, int file_offset);
template <typename T>
block_obj<T> * readIncompleteBlockFromFile(const column_meta& col_meta, std::string& table_name, std::string& col_name);
template <typename T>
void processColumnData(std::vector<data<T>> &newRecords, std::string& table_name, std::string& col_name);

template <typename T>
void remove_deleted_values(std::vector<data<T>> * all_data_vec);

template <typename T>
data<T> * get_block_data(std::ifstream &file, const int block_no , const block_meta<T> &blk_meta);

template <typename T>
data<T> * get_block_data(const std::string &table_name, const std::string &col_name, const int block_no , const block_meta<T> &blk_meta);

template <typename T>
std::vector<data<T>> * get_block_data(std::string table_name, std::string col_name, const std::vector<int> &block_nos , const std::pair<block_meta<T> *, int> & blk_meta);

template <typename T>
std::vector<data<T>> * get_data_with_rowid(const std::string &table_name, const std::string &col_name, RowID_vector filterd_rows);


template<typename T>
void dump_blocks_chunk(std::string table_name, std::string col_name, std::vector<block_obj<T>> & data_chunk, int block_no);

template<typename T>
std::vector<block_obj<T>> * get_blocks_chunk(std::string table_name, std::string col_name, int block_no, const column_meta &col_meta);

#endif