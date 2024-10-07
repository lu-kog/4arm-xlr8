
#ifndef meta_h
#define meta_h 09



#define DBINT 1
#define DBFLOAT 2
#define DBLONG 3
#define DBDOUBLE 4
#define DBSTRING 5
#define DBCHAR 6

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




struct column_meta
{
    int total_records;
    int data_type;
    int no_block;
};



template <typename dt>
struct block_meta
{
    int count;
    dt min;
    dt max; 
    dt sum;
};


template <typename dt>
struct column_obj
{
    block_meta<dt> meta;
    // data<dt>* all_data;
    union all_data{
        std::vector<data<int>> int_data;
        std::vector<data<float>> float_data;
        std::vector<data<long>> long_data;
        std::vector<data<double>> double_data;
        std::vector<data<Dbstr>> str_data;
        std::vector<data<char>> char_data;
    }
};



struct schema_meta{
    int number_of_columns;
    int* data_type;
    // str* fields; 
    std::pair <int,char *> *fields;
    
};


#endif