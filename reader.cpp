#include "meta.h"
#include "fileHandler.cpp"
#include "cred.cpp"
#include "meta_handler.cpp"
#include "string_handler.cpp"

#include <string>
#include <vector>
#include <iostream>
#include <fstream>

template <typename T>
data<T> * get_block_data(const std::string &table_name, const std::string &col_name, const int block_no , const block_meta<T> &blk_meta){
    
    int offset = sizeof(column_meta) + ((sizeof(block_meta<T>) + (sizeof(data<T>) * 100))  * (block_no-1))   + sizeof(block_meta<T>);

    data<T> * all_data_frm_blk = new data<T>[blk_meta.count];

    readBinaryFile(get_path() + table_name + "/"+col_name, (char *) all_data_frm_blk,  (blk_meta.count * sizeof(data<T> )),offset);

    return all_data_frm_blk;
    
    

} 


//for getting block meta pointer and count of block meta
template <typename T>
std::pair<block_meta<T> *, int> get_all_block_meta(const std::string &table_name, const std::string &col_name){
    column_meta *curr_col_meta = get_column_meta(table_name, col_name);

    int no_blocks = curr_col_meta->no_block;
    int offset_for_file = sizeof(column_meta);
    int offset_for_pointer = 0;
    block_meta<T> *all_blk_meta  = new block_meta<T>[no_blocks];

    std::ifstream column_file(get_path()+table_name+"/"+col_name,std::ios::binary);
    
    for (size_t i = 0; i < no_blocks; i++)
    {
        readBinaryFile((char *) (all_blk_meta + offset_for_pointer), sizeof(block_meta<T>) ,offset_for_file,column_file);

        offset_for_file += sizeof(block_meta<T>) + (sizeof(data<T>) * 100); 
        offset_for_pointer += sizeof(block_meta<T>);
    }


    delete curr_col_meta;
    return {all_blk_meta,no_blocks};
    

}



#if 1

int main(int argc, char const *argv[])
{
    get_home_folder();
    block_meta<Dbstr> blk_meta;

    blk_meta.count = 20;



    std::string table_name = "kcc";

    std::string column_name = "Najimi";


    

    std::pair<block_meta<Dbstr> *,int> all_block_meta = get_all_block_meta<Dbstr>(table_name,column_name);



    // std::cout << str_file_reader(table_name,all_block_meta.first->min) << std::endl;



    for (size_t i = 0; i < all_block_meta.second; i++)
    {
        
        data<Dbstr> * all_data = get_block_data(table_name, column_name, (i+1) , all_block_meta.first[i]);

        for (size_t j = 0; j < all_block_meta.first[i].count ; j++)
        {
            std::cout <<  str_file_reader(table_name, all_data[j].data) << std::endl;
        }
        

        delete[] all_data;
    }
    return 0;
}


#endif