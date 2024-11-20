
#pragma once

#ifndef _INSERT
#define _INSERT 0

#include <iostream>
#include "cred.cpp"
#include "fileHandler.cpp"
#include "meta_handler.cpp"
#include <cstring>

#include "csv_parser.cpp"
#include "converter.cpp"



void createBlocks(column_obj & clm, std::string& table_name, std::string& col_name);

template <typename T>
void processColumnData(std::vector<data<T>> &newRecords, std::string& table_name, std::string& col_name);

template <typename T>
void dump_new_records(std::vector<block_obj<T>> & new_blocks, std::string &table_name, std::string &col_name, int file_offset);
template <typename T>
block_obj<T> * readIncompleteBlockFromFile(const column_meta& col_meta, std::string& table_name, std::string& col_name);

void insertIntoTable(std::vector<column_obj> & columns_to_insert, std::string& table_name, schema_meta & table_schema){

    for (int i = 0; i < columns_to_insert.size(); i++)
    {
        std::string col_name(table_schema.fields[i].second, table_schema.fields[i].first); // While creating for thread handle the col_name scope; 
        createBlocks(columns_to_insert.at(i), table_name, col_name);
    }
    
}


void createBlocks(column_obj & clm, std::string& table_name, std::string& col_name){

    column_meta meta_to_process = clm.meta;

    int data_type = clm.meta.data_type;
    
    switch (data_type) {
        case DBINT:
            processColumnData<int>(*clm.all_data.int_data, table_name, col_name);

            {auto vec = *clm.all_data.int_data;
            break;}
            
        case DBCHAR:
            processColumnData<char>(*clm.all_data.char_data, table_name, col_name);
            break;
        case DBLONG:
            processColumnData<long>(*clm.all_data.long_data, table_name, col_name);
            break;
        case DBDOUBLE:
            processColumnData<double>(*clm.all_data.double_data, table_name, col_name);
            break;
        case DBSTRING:
            processColumnData<Dbstr>(*clm.all_data.str_data, table_name, col_name);
            break;
        case DBFLOAT:
            processColumnData<float>(*clm.all_data.float_data, table_name, col_name);
            break;
        default:
            std::cerr << "Unsupported data type" << std::endl;
            break;
    }
    

}


template <typename T>
void processColumnData(std::vector<data<T>> &newRecords, std::string& table_name, std::string& col_name) {
    column_meta * col_meta = get_column_meta(table_name, col_name);

    std::vector<block_obj<T>> new_blocks;

    auto it = newRecords.begin();

    bool is_string = typeid(T) == typeid(Dbstr);
    
    // If unfinished block
    if (col_meta->total_records % records_limit != 0) {
        block_obj<T> * lastBlock = readIncompleteBlockFromFile<T>(*col_meta, table_name, col_name);

        while (lastBlock->all_data.size() < records_limit && it != newRecords.end()) {
            lastBlock->all_data.push_back(*it);

            // Meta updation.. skip if it string
            if (!is_string)
            {
                if ((it->data) > lastBlock->meta.max)
                {
                    lastBlock->meta.max = it->data;
                }else if ((it->data) < lastBlock->meta.min)
                {
                    lastBlock->meta.min = it->data;
                }
            }
            it++;
            
        }

        // update last block meta
        lastBlock->meta.count = lastBlock->all_data.size();
        col_meta->no_block -=1;
        
        new_blocks.push_back(*lastBlock);
        delete lastBlock;
    }

    auto begin = newRecords.begin();
    auto end = newRecords.end();




    // Process remaining records in blocks of 100
    while (it != end) {
        block_obj<T> block;
        for (int i = 0; i < records_limit && it != newRecords.end(); i++) {

            block.all_data.push_back(*it);
            
            // Meta calculation.. skip if it string
            if (!is_string){
                if ((it->data) > block.meta.max)
                {
                    block.meta.max = it->data;
                }
                if ((it->data) < block.meta.min)
                {
                    block.meta.min = it->data;
                }
            }
            it++;
        }

        block.meta.count = block.all_data.size();

        new_blocks.push_back(block);

    }
    
    int records_size = records_limit * sizeof(data<T>);

    bool isCompleteDataSet = (col_meta->total_records % 100) == 0;

    // file offset should be starting point of the block.
    // if pending block, offset is it's starting point
    // if the column already have some complete blocks, pointer should be next to all the blocks.

    int file_offset = sizeof(column_meta) + ( (isCompleteDataSet ? col_meta->no_block : (col_meta->no_block - 1)) * sizeof(block_meta<T>));
    file_offset += ( (isCompleteDataSet ? col_meta->no_block : (col_meta->no_block - 1)) * records_size );


    if (col_meta->no_block == 0)
    {
        file_offset = sizeof(column_meta);
    }
    

    col_meta->total_records += newRecords.size();
    col_meta->no_block += new_blocks.size();


    /*
        overwrite column_meta
        overwrite last block & dump newBlocks
        clear memory()
    */
    
   write_column_meta(table_name, col_name, *col_meta);
   dump_new_records(new_blocks, table_name, col_name, file_offset);

    delete col_meta;

}

template <typename T>
void dump_new_records(std::vector<block_obj<T>> & new_blocks, std::string &table_name, std::string &col_name, int file_offset){
    int blocks_count = new_blocks.size();
    int block_meta_size = sizeof(block_meta<T>);
    int block_data_size = 100 * sizeof(data<T>);

    int buffer_size = (blocks_count * block_meta_size) + (blocks_count * block_data_size);
    char * buffer =  (char *) malloc(buffer_size);
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
    
    delete buffer;
}



template <typename T>
block_obj<T> * readIncompleteBlockFromFile(const column_meta& col_meta, std::string& table_name, std::string& col_name) {
    /* Get path & read from file */
    std::string file_path = get_path() + table_name + "/" + col_name;
    int offset = sizeof(column_meta) + ((col_meta.no_block - 1) * sizeof(block_meta<T>)) + ((col_meta.no_block - 1) * 100 * sizeof(data<T>));
    

    block_obj<T> *lastBlock = new block_obj<T>();

    // read last block meta
    readBinaryFile(file_path, (char*) (&lastBlock->meta), sizeof(block_meta<T>), offset);

    std::vector<data<T>> tempData(lastBlock->meta.count); // allocate for 100 entries (per block)
    
    // read from the point next to the block meta to count of entries.
    readBinaryFile(file_path, (char*) (tempData.data()), lastBlock->meta.count * sizeof(data<T>), offset + sizeof(block_meta<T>));

    lastBlock->all_data = std::move(tempData);

    return lastBlock;
}





void print_data(column_obj column){
    switch (column.meta.data_type)
    {
    case DBINT:
        std::cout<<"Int"<<"\n";
        for (size_t i = 0; i < column.meta.total_records; i++)
        {
            std::cout<<column.all_data.int_data[0][i].row_id<<std::endl;
        }
        break;
    case DBCHAR:
        std::cout<<"Char"<<"\n";
        for (size_t i = 0; i < column.meta.total_records; i++)
        {
            std::cout<<column.all_data.char_data[0][i].data<<std::endl;
        }
        break;
    case DBLONG:
        std::cout<<"Long"<<"\n";
        for (size_t i = 0; i < column.meta.total_records; i++)
        {
            std::cout<<column.all_data.long_data[0][i].data<<std::endl;
        }
        break;
    case DBDOUBLE:
        std::cout<<"Double"<<"\n";
        for (size_t i = 0; i < column.meta.total_records; i++)
        {
            std::cout<<column.all_data.double_data[0][i].data<<std::endl;
        }
        break;
    case DBSTRING:
        std::cout<<"String"<<"\n";

        for(size_t i = 0; i < column.meta.total_records; i++)
        {
            std::cout<< column.all_data.str_data[0][i].data.file <<"\t" << column.all_data.str_data[0][i].data.offset_start <<"\t"<<  column.all_data.str_data[0][i].data.size  <<std::endl;
        }
        break;
    case DBFLOAT:
        std::cout<<"Float"<<"\n";
        for (size_t i = 0; i < column.meta.total_records; i++)
        {
            std::cout<<column.all_data.float_data[0][i].data<<std::endl;
        }
        break;
    
    default:
        break;
    }

    
    
}




void insert(std::string table_name, std::string csv_path){
    std::ifstream csvFile;
    csvFile.open(csv_path);
    std::vector<std::string> * parsed = parseCSV(csvFile);

    schema_meta * schema = read_schema(table_name);


    std::vector<column_obj> table_data; 
    type_casting(*parsed,*schema, table_data,table_name);
    insertIntoTable(table_data,table_name, *schema);


}





#if 1
int main(int argc, char const *argv[])
{




    get_home_folder();



#if 1
    std::string tableName = "kcc";
    int numColumns;
    std::vector<std::string> columnNames = {"Komi", "Najimi", "Tadano"};
    std::vector<int> columnDataTypes = {DBINT,DBSTRING,DBFLOAT};



    create_table(tableName,columnNames,columnDataTypes);
#endif


    insert("kcc","/home/ajith-zstk355/temp.csv");
    return 0;



    
}


#endif

#endif