#pragma once

#ifndef _DATA_F
#define _DATA_F 69


#include "meta.h"
#include "fileHandler.cpp"
#include "cred.cpp"
#include "meta_handler.cpp"
#include "string_handler.cpp"
#include "Node.h"

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <stdexcept> 




std::string get_file_path (const std::string& table_name,  const std::string &col_name){

    std::string path = get_path() + table_name + "/"+col_name;


    return path;


}



template <typename T>
data<T> * get_block_data(std::ifstream &file, const int block_no , const block_meta<T> &blk_meta){



    if (block_no <= 0) {
        throw std::invalid_argument("block_no must be greater than 0");
    }  

    int column_meta_size = sizeof(column_meta); 
    int block_meta_size = sizeof(block_meta<T>);
    int data_size = sizeof(data<T>);
    int block_size =   block_meta_size + (data_size * 100);

    int offset =  column_meta_size + ((block_no-1) * block_size);

    offset += block_meta_size;  // for current block meta

    data<T> * all_data_frm_blk = new data<T>[blk_meta.count];

    readBinaryFile((char *) all_data_frm_blk,  (blk_meta.count * sizeof(data<T> )),offset,file);

    return all_data_frm_blk;

} 




template <typename T>
data<T> * get_block_data(const std::string &table_name, const std::string &col_name, const int block_no , const block_meta<T> &blk_meta){




    std::ifstream file(get_file_path(table_name,col_name),std::ios::binary);
     
    data<T> * block_data = get_block_data(file,block_no,blk_meta);

    file.close();


    return block_data;
} 









template <typename T>
std::vector<data<T>> * get_block_data(std::string table_name, std::string col_name, const std::vector<int> &block_nos , const std::pair<block_meta<T> *, int> & blk_meta){
    std::ifstream file(get_file_path(table_name,col_name),std::ios::binary);
     
    std::vector<data<T>> *all_data = new std::vector<data<T>>;

    for (int blk_no : block_nos)
    {

        block_meta<T> & single_blk_meta = blk_meta.first[blk_no-1];

        data<T> * block_data = get_block_data(file,blk_no,single_blk_meta);

        all_data->insert(all_data->end(),block_data,block_data+single_blk_meta.count);


        delete []block_data;
        
    }
    
    int size = all_data->size();

    file.close();

    return all_data;
}

//for getting block meta pointer and count of block meta
template <typename T>
std::pair<block_meta<T> *, int> get_all_block_meta(const std::string &table_name, const std::string &col_name){
    column_meta *curr_col_meta = get_column_meta(table_name, col_name);

    int no_blocks = curr_col_meta->no_block;
    int column_meta_size = sizeof(column_meta);
    int block_size = sizeof(block_meta<T>) + (100 * sizeof(data<T>));

    int offset_for_file = column_meta_size;
    
    block_meta<T> *all_blk_meta  = new block_meta<T>[no_blocks];

    std::ifstream column_file(get_file_path(table_name,col_name),std::ios::binary);
    
    for (size_t i = 0; i < no_blocks; i++)
    {
        readBinaryFile((char *) (all_blk_meta + i), sizeof(block_meta<T>) ,offset_for_file,column_file);

        offset_for_file +=  block_size; 
    }


    delete curr_col_meta;
    return {all_blk_meta,no_blocks};
    

}


template <typename T>
std::vector<data<T>> * get_data_with_rowid(const std::string &table_name, const std::string &col_name, RowID_vector filterd_rows){
    std::pair<block_meta<T> *,int> all_block_meta = get_all_block_meta<T>(table_name,col_name);






    std::ifstream col_file(get_file_path(table_name,col_name),std::ios::binary);



    std::vector<data<T>> * all_data = new std::vector<data<T>>;
    
    std::vector<data<T>> &ad = *all_data;
    for (size_t i = 0; i < all_block_meta.second; i++)
    {
        data<T> * blk_data = get_block_data(col_file, (i+1) , all_block_meta.first[i]);

        data<T> * data_count = blk_data+(all_block_meta.first[i].count);
        all_data->insert(all_data->end(),blk_data,data_count);
        delete[] blk_data;
    }

    

    

    if(filterd_rows != nullptr){

        if (all_data->size() < filterd_rows->size()) {
            throw std::runtime_error("The number of filtered rows exceeds the total number of rows in the column.");
        }



        std::vector<data<T>> temp_vec(filterd_rows->size());

        int size = temp_vec.size(); 

        for (size_t i = 0; i < size; i++)
        {
            temp_vec[i] = (*all_data)[(*filterd_rows)[i]];
        }        

        all_data->swap(temp_vec);
    }
    col_file.close();

    return all_data;    


}

#endif