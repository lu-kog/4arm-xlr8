#include "../include/Schema.h"

schema_meta* read_schema(std::string table_name){

    std::string f_name = get_path()+table_name+"/"+table_name + ".schema";
    int file_size = get_size(f_name);
    char * buffer = (char *) malloc(file_size);
    char* temp_buffer = buffer;
    readBinaryFile(f_name,buffer,file_size,0);


    schema_meta* table_schema = new schema_meta;

    //number of col cpy
    int * number_of_col = (int *) temp_buffer;
    table_schema->number_of_columns = *number_of_col;
    temp_buffer = temp_buffer+sizeof(int); // move the pointer
    
    // parse the column data types 
    int *column_dt = new int[*number_of_col];
    memcpy(column_dt,temp_buffer, ((*number_of_col) *sizeof(int)));
    table_schema->data_type = column_dt;
    temp_buffer = temp_buffer+ (table_schema->number_of_columns*sizeof(int));

    //parsing the column names
    std::pair <int, char*> *column_names = new std::pair<int,char*> [table_schema->number_of_columns];
    for (size_t i = 0; i < table_schema->number_of_columns; i++)
    {   
        // get the size of the string
        int *temp = (int *) (temp_buffer);
        column_names[i].first = *temp;
        temp_buffer = temp_buffer+sizeof(int) ;

        //copying the col_name as a string 
        char *temp_name = new char[*temp+1];
        memcpy(temp_name, temp_buffer, *temp);
        temp_name[*temp] = '\0'; // Add null terminator to the end of the string.
        column_names[i].second = temp_name;  
        temp_buffer = temp_buffer + (*temp);      

    }
    table_schema->fields = column_names;
    free(buffer);
    return table_schema;
}

void write_schema(std::string table_name,std::vector <std::string> &columns_name, std::vector <int> &columns_data_type){
    char * buffer; 

    int size_for_malloc = sizeof(int); // total_columns
    size_for_malloc += (columns_data_type.size() * sizeof(int)); //data type vector 

    for (std::string &str : columns_name)
    {
        size_for_malloc += sizeof(int);
        size_for_malloc += str.size();
    }
    
    buffer = (char *) malloc(size_for_malloc);

    char * temp_buffer = buffer;

    //assigning total_columns
    int * column_count = (int *) temp_buffer;
    *column_count = columns_name.size();
    temp_buffer = temp_buffer+sizeof(int);

    //assigning data type vector
    int d_t_vec_size = sizeof(int) * columns_data_type.size();
    memcpy(temp_buffer,columns_data_type.data(),d_t_vec_size);
    temp_buffer = temp_buffer+ d_t_vec_size;
    

    //assigning the column names to the schema
    for (std::string &str : columns_name)
    {
        //size of the string
        int * str_size = (int *) temp_buffer;
        *str_size = str.size();
        temp_buffer = temp_buffer+sizeof(int); 

        //string 
        memcpy(temp_buffer,str.data(),(*str_size));
        temp_buffer = temp_buffer+(*str_size);
    }

    std::string schema_path = get_path()+table_name+"/"+table_name + ".schema";
    writeBinaryFile(schema_path,buffer,size_for_malloc,0);

    std::cout << "Schema Written successfully" << std::endl;
}


