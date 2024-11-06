#ifndef _cred_h
#define _cred_h 1

#include "meta.h" 
#include "fileHandler.cpp"
// #include <vector>
#include <cstring>
#include <filesystem>
#include <cstdlib>


#include <iostream>

bool create_folder(std::string name);

template <typename dt>
void write_meta(std::string path, column_meta meta, block_meta <dt> bloc_m, int offset);

static std::string path;

void get_home_folder(){
    path = std::getenv("HOME");

    path += "/oursql";

    if (!std::filesystem::exists(path)){
        if (std::filesystem::create_directory(path)) {
            std::cout << "Folder created successfully at: " << path << std::endl;
        }
    }

    path += "/";
}


std::string get_path(){
    return path;
}

void create_table(std::string table_name,std::vector <std::string> &columns_name, std::vector <int> &columns_data_type){

    if(!create_folder(table_name)){
        std::cout<<"Table already Exisit\n";
        return;
    };

    char * buffer;
    schema_meta new_table;
    new_table.number_of_columns = columns_name.size();
    new_table.data_type = &columns_data_type[0];

    int data_type_vec_size = new_table.number_of_columns*4;


    int total_buffer_size = 0;


    int m_size = 0;

    for (size_t i = 0; i < new_table.number_of_columns; i++)
    {
        m_size += (4 +columns_name[i].size());
    }


    total_buffer_size = m_size+data_type_vec_size+ sizeof(new_table);

    buffer = (char *) malloc(total_buffer_size);

    memcpy(buffer, &new_table, sizeof(new_table));

    memcpy(buffer+sizeof(new_table) , new_table.data_type , data_type_vec_size);

    //to track the memory occupied 

    int temp_size = sizeof(new_table) + data_type_vec_size;

    for (size_t i = 0; i < new_table.number_of_columns; i++)
    {
        int len = columns_name[i].size();
        memcpy(buffer+temp_size, &len,  4);

        memcpy(buffer+temp_size+4 ,&columns_name[i][0],columns_name[i].size());


        temp_size += (4 +columns_name[i].size());
    }






    writeRecords(path+table_name+"/"+table_name + ".schema",buffer,total_buffer_size,0);

#if 1
    for (size_t i = 0; i < new_table.number_of_columns; i++)
    {
        int offset = 0;
        switch(new_table.data_type[i]) // switch (new_table.data_type[i])
        {
        case DBINT:{
            std::cout<<"Int"<<"\n";
            column_meta meta{0,new_table.data_type[i],0};
            block_meta<int> block_meta_for_int;

            write_meta(path+table_name+"/"+columns_name[i], meta, block_meta_for_int,0);
            

            break;
            }
        case DBCHAR:{
            std::cout<<"Char"<<"\n";

            column_meta meta{0,new_table.data_type[i],0};
            block_meta<char> block_meta_for_char;

            write_meta(path+table_name+"/"+columns_name[i], meta, block_meta_for_char,0);
            break;
            }
        case DBLONG:{
            std::cout<<"Long"<<"\n";
            


            column_meta meta{0,new_table.data_type[i],0};
            block_meta<long> block_meta_for_long;

            write_meta(path+table_name+"/"+columns_name[i], meta, block_meta_for_long,0);
            break;
            }
        case DBDOUBLE:{
            std::cout<<"Double"<<"\n";


            column_meta meta{0,new_table.data_type[i],0};
            block_meta<double> block_meta_for_double;

            write_meta(path+table_name+"/"+columns_name[i], meta, block_meta_for_double,0);
            break;
            }
        case DBSTRING:{
            std::cout<<"String"<<"\n";


            column_meta meta{0,new_table.data_type[i],0};

           

            block_meta<Dbstr> block_meta_for_string;

            write_meta(path+table_name+"/"+columns_name[i], meta, block_meta_for_string,0);
            break;
            }
        case DBFLOAT:{
            std::cout<<"Float"<<"\n";


            column_meta meta{0,new_table.data_type[i],0};
            block_meta<int> block_meta_for_float;

            write_meta(path+table_name+"/"+columns_name[i], meta, block_meta_for_float,0);
            break;
            }
        default:
            break;
        }
    }
    

#endif





    free(buffer);
    
}



template <typename dt>
void write_meta(std::string path, column_meta meta, block_meta <dt> bloc_m, int offset ){
    writeRecords(path, (char *) &meta, sizeof(meta), offset);
    // char * buffer = malloc()

    // writeRecords(path, (char *) &bloc_m, sizeof(bloc_m), offset);
}



schema_meta* read_schema(std::string table_name){

    std::string f_name = path+table_name+"/"+table_name + ".schema";


    int copied_buffer = 0;


    int file_size = get_size(f_name);

    char * buffer = (char *) malloc(file_size);


    readBinaryFile(f_name,buffer,file_size,0);


    schema_meta* table_schema = new schema_meta;



    memcpy(table_schema,buffer,sizeof(*table_schema));


    copied_buffer += sizeof(*table_schema);

    int *column_dt = new int[table_schema->number_of_columns];

    memcpy(column_dt,buffer+copied_buffer, (table_schema->number_of_columns*4));


    table_schema->data_type = column_dt;


    copied_buffer += (table_schema->number_of_columns*4);
    //parsing the column names
    std::pair <int, char*> *column_names = new std::pair<int,char*> [table_schema->number_of_columns];



    for (size_t i = 0; i < table_schema->number_of_columns; i++)
    {
        int *temp = (int *) (buffer+copied_buffer);


        copied_buffer += 4;

        char *temp_name = new char[*temp+1];


        memcpy(temp_name, buffer+copied_buffer, *temp);



        column_names[i].first = *temp;
        column_names[i].second = temp_name;  


        copied_buffer += (*temp);      

    }
    table_schema->fields = column_names;

    return table_schema;
}



bool create_folder(std::string name){
    std::filesystem::path dir(path+name);
    if (std::filesystem::create_directory(dir)) {
        return true ;
    }
    else 
        return false;
}


#endif