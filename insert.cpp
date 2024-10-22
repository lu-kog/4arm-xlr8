
#pragma once

#ifndef _INSERT
#define _INSERT 0

#include <iostream>
#include "cred.h"
#include "fileHandler.cpp"
#include "meta_handler.cpp"
#include <cstring>

#define records_limit 100


void insertIntoTable(std::vector<column_obj> & columns_to_insert, std::string& table_name, schema_meta & table_schema){

    for (int i = 0; i < columns_to_insert.size(); i++)
    {
        createBlocks(columns_to_insert.at(i), table_name, table_schema.fields[i].second);
    }
    
}


void createBlocks(column_obj & clm, std::string& table_name, std::string& col_name){

    column_meta meta_to_process = clm.meta;

    int data_type = clm.meta.data_type;
    
    switch (data_type) {
        case DBINT:
            processColumnData<int>(*clm.all_data.int_data, meta_to_process, table_name, col_name);
            break;
        case DBCHAR:
            processColumnData<char>(*clm.all_data.char_data, meta_to_process, table_name, col_name);
            break;
        case DBLONG:
            processColumnData<long>(*clm.all_data.long_data, meta_to_process, table_name, col_name);
            break;
        case DBDOUBLE:
            processColumnData<double>(*clm.all_data.double_data, meta_to_process, table_name, col_name);
            break;
        case DBSTRING:
            processColumnData<std::string>(*clm.all_data.str_data, meta_to_process, table_name, col_name);
            break;
        case DBFLOAT:
            processColumnData<float>(*clm.all_data.float_data, meta_to_process, table_name, col_name);
            break;
        default:
            std::cerr << "Unsupported data type" << std::endl;
            break;
    }
    

}


template <typename T>
void processColumnData(std::vector<data<T>> &newRecords, std::string& table_name, std::string& col_name) {
    column_meta * col_meta = get_column_meta(table_name, col_name);

    std::vector<block_obj> new_blocks;
    
    auto it = newRecords.begin();
    
    // If unfinished block
    if (col_meta.total_records % records_limit != 0) {
        block_obj<T> * lastBlock = readIncompleteBlockFromFile<T>(col_meta, table_name, col_name);

        while (lastBlock.size() < records_limit && it != newRecords.end()) {
            lastBlock->all_data.push_back(*it++);

            // Meta updation.. skip if it string
            if (typeid(dt) != typeid(Dbstr))
            {
                if (it->data > lastBlock->meta.max)
                {
                    lastBlock->meta.max = it->data;
                }else if (it->data < lastBlock->meta.min)
                {
                    lastBlock->meta.min = it->data;
                }
            }
            
            
        }

        // update meta
        lastBlock->meta.count = lastBlock->all_data.size();
        
        new_blocks.push_back(*lastBlock);
        delete lastBlock;
    }

    // Process remaining records in blocks of 100
    while (it != it.end()) {
        block_obj<T> newBlock;
        for (int i = 0; i < records_limit && it != newRecords.end(); i++) {

            newBlock.all_data.push_back(*it++);
            
            // Meta updation.. skip if it string
            if (typeid(dt) != typeid(Dbstr)){
                if (it->data > newBlock.meta.max)
                {
                    newBlock.meta.max = it->data;
                }else if (it->data < newBlock.meta.min)
                {
                    newBlock.meta.min = it->data;
                }
            }
            
        }

    }
    
    
    int file_offset = sizeof(column_meta) + ((col_meta.no_blocks - 1) * sizeof(block_meta<T>)) + ((col_meta.no_blocks - 1) * 100 * sizeof(data<T>));

    col_meta.total_records += newRecords.size();
    col_meta->no_block += new_blocks.size() - 1;


    /*
        overwrite column_meta
        overwrite last block
        dump newBlocks
        clear memory()
    */
    
   write_column_meta(table_name, col_name, col_meta);
   dump_new_records(new_blocks, table_name, col_name, file_offset);


}

template <typename T>
void dump_new_records(std::vector<block_obj<T>> & new_blocks, std::string &table_name, std::string &col_name, int file_offset){
    int blocks_count = new_blocks.size();
    int block_meta_size = sizeof(block_meta<T>);
    int block_data_size = 100 * sizeof(data<T>);

    int buffer_size = (blocks_count * block_meta_size) + (blocks_count * block_data_size);
    char * buffer = malloc(buffer_size);
    int offset = 0;

    for (block_obj<T> blk : new_blocks)
    {
        memcpy(buffer+offset, &blk.meta, block_meta_size);
        offset += block_meta_size;
        memcpy(buffer+offset, blk.all_data.data(), blk.all_data.size() * sizeof(data<T>)); // case: unfinished block
        offset += blk.all_data.size() * sizeof(data<T>);
    }
    
    std::string fileName = get_path() + table_name + "/"+col_name;
     
    writeRecords(fileName,buffer, buffer_size, file_offset);
    
}



template <typename T>
block_obj * readIncompleteBlockFromFile(const column_meta& col_meta, std::string& table_name, std::string& col_name) {
    /* Get path & read from file */
    std::string file_path = get_home_folder() + "/" + table_name + "/" + col_name;
    int offset = sizeof(column_meta) + ((col_meta.no_blocks - 1) * sizeof(block_meta<T>)) + ((col_meta.no_blocks - 1) * 100 * sizeof(data<T>));

    block_obj<T> *lastBlock = new block_obj<T>();

    // read last block meta
    readBinaryFile(file_path, (char*) (&lastBlock->meta), sizeof(block_meta<T>), offset);

    std::vector<data<T>> tempData(100); // allocate for 100 entries (per block)
    
    // read from the point next to the block meta to count of entries.
    readBinaryFile(file_path, (char*) (tempData.data()), lastBlock->meta.count * sizeof(data<T>), offset + sizeof(block_meta<T>));

    lastBlock->all_data = std::move(tempData);

    return lastBlock;
}




#endif