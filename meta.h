
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




template <typename dt>
struct column_meta
{
    int count;
    dt min;
    dt max; 
    dt sum;
};


template <typename dt>
struct column_obj
{
    column_meta<dt> meta;
    // data<dt>* all_data;
    union all_data{
        std::vector<int> int_data;
        std::vector<float> float_data;
        std::vector<long> long_data;
        std::vector<double> double_data;
        std::vector<Dbstr> str_data;
        std::vector<char> char_data;
    }
};



struct schema_meta{
    int number_of_columns;
    int* data_type;
    // str* fields; 
    std::pair <int,char *> *fields;
    
};


#endif