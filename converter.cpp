#ifndef conv
#define conv 1


#include <vector>
#include "meta.h"
#include <string>
#include <iostream>
#include "string_handler.cpp"
#include "meta_handler.cpp"
#include <fstream>

void create_vec(column_obj &column)
{
    switch (column.meta.data_type)
    {
    case DBINT:
    {
        column.all_data.int_data = new std::vector<data<int>>;
        break;
    }
    case DBCHAR:
    {
        column.all_data.char_data = new std::vector<data<char>>;

        break;
    }
    case DBDOUBLE:
    {
        column.all_data.double_data = new std::vector<data<double>>;

        break;
    }
    case DBSTRING:
    {
        column.all_data.str_data = new std::vector<data<Dbstr>>;
        ;
        break;
    }
    case DBFLOAT:
    {
        column.all_data.float_data = new std::vector<data<float>>;
        break;
    }
    case DBLONG:
    {
        column.all_data.long_data = new std::vector<data<long>>;
        break;
    }
    }
}



void delete_vec(column_obj &column)
{
    switch (column.meta.data_type)
    {
    case DBINT:
    {
        delete column.all_data.int_data;
        break;
    }
    case DBCHAR:
    {
        delete column.all_data.char_data;

        break;
    }
    case DBDOUBLE:
    {
        delete column.all_data.double_data;

        break;
    }
    case DBSTRING:
    {
        delete column.all_data.str_data;
        break;
    }
    case DBFLOAT:
    {
        delete column.all_data.float_data;
        break;
    }
    case DBLONG:
    {
        delete column.all_data.long_data;
        break;
    }
    }
}


void initilaize_column_objs(const schema_meta &schema, std::vector<column_obj> &table_data){
    for (size_t i = 0; i < schema.number_of_columns; i++)
    {
        column_obj temp;

        temp.meta.data_type = schema.data_type[i];
        temp.meta.no_block = 0;

        temp.meta.total_records = (data_as_string.size() / schema.number_of_columns);

        create_vec(temp);
        table_data.push_back(temp);
    }
}




/*
type_casting methods gets string as data from csv like

    1  jith  6.9
    2  komi  4.20

form the table already created using `schema_meta`
*/

std::vector<column_obj> &type_casting(std::vector<std::string> &data_as_string, schema_meta &schema_for_table, std::vector<column_obj> &table_data, const std::string &table_name)
{

    int number_of_data = data_as_string.size();


    // column_meta * column_meta = 


    initilaize_column_objs(schema_for_table,table_data);


    // when the number of columns and given data doesn't match
    //  no column = 3
    //  data = 5 --
    if ((number_of_data % schema_for_table.number_of_columns) != 0)
    {
        table_data.resize(0);
        std::cout << schema_for_table.number_of_columns << "\t" << number_of_data << std::endl;

        return table_data; // throw exepction here
    }
    else
    {
        int file_no = 1;

        std::ofstream str_file; 
        get_file_to_write(file_no,table_name, str_file);


        for (size_t i = 0; i < number_of_data; i++)
        {
            unsigned int row_id = (i / schema_for_table.number_of_columns);
            int column_no = (i % schema_for_table.number_of_columns);
            switch (schema_for_table.data_type[column_no])
            {
            case DBINT:
            {
                data<int> datum{row_id, 0 ,std::stoi(data_as_string[i])}; // type casting

                (*(table_data[column_no].all_data.int_data)).push_back(datum);

                
                break;
            }
            case DBCHAR:
            {
                data<char> datum{row_id , 0 , data_as_string[i][0]};
                (*(table_data[column_no].all_data.char_data)).push_back(datum);
                break;
            }
            case DBDOUBLE:
            {
                data<double> datum{row_id , 0 , std::stod(data_as_string[i])};
                (*(table_data[column_no].all_data.double_data)).push_back(datum);
                break;
            }
            case DBSTRING:
            {

                Dbstr string_val = str_file_writer(str_file,data_as_string[i],file_no);

                data<Dbstr> datum{row_id , 0 , /*that string modifing methods go here;*/ string_val};
                (*(table_data[column_no].all_data.str_data)).push_back(datum);

                string_write_file_size_check(file_no, table_name, str_file);

                break;
            }
            case DBFLOAT:
            {
                data<float> datum{row_id , 0 , std::stof(data_as_string[i])};
                (*(table_data[column_no].all_data.float_data)).push_back(datum);
                break;
            }
            case DBLONG:
            {

                data<long> datum{row_id , 0 , std::stol(data_as_string[i])};
                (*(table_data[column_no].all_data.long_data)).push_back(datum);
                break;
            }
            }
        }

        str_file.close();
    }

    // std::cout << "Jith" << std::endl;

        

    return table_data;
}



#endif