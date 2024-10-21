#include <vector>
#include "meta.h"
#include <string>
#include <iostream>



void create_vec(column_obj & column){
    switch (column.meta.data_type)
    {
    case DBINT:{
        column.all_data.int_data = new std::vector<data<int>>;
        break;
        }
    case DBCHAR:{
        column.all_data.char_data = new std::vector<data<char>>;
        
        break;
        }
    case DBDOUBLE:{
        column.all_data.double_data = new std::vector<data<double>>;
        
        break;
        }
    case DBSTRING:{
        column.all_data.str_data = new std::vector<data<Dbstr>>;;
        break;
        }
    case DBFLOAT:{
        column.all_data.float_data = new std::vector<data<float>>;
        break;
        }
    }
}



std::vector<column_obj> &type_casting(std::vector <std::string> data_as_string, schema_meta schema_for_table, std::vector<column_obj> &table_data){
    
    int number_of_data = data_as_string.size();

    for (size_t i = 0; i < schema_for_table.number_of_columns; i++)
    {
        column_obj temp;

        temp.meta.data_type = schema_for_table.data_type[i];
        temp.meta.no_block = -1;

        temp.meta.total_records = (data_as_string.size()/schema_for_table.number_of_columns);
        
        
        create_vec(temp);
        table_data.push_back(temp);




    }
    


    //when the number of columns and given data doesn't match 
    // no column = 3
    // data = 5 --     
    if((schema_for_table.number_of_columns%number_of_data) != 0){
        table_data.resize(0);
        std::cout<<"No data"<<std::endl;

        return table_data;   //throw exepction here
    } 
    else{
        for (size_t i = 0; i < number_of_data; i++)
        {
            unsigned int row_id = (i/schema_for_table.number_of_columns);
            int column_no = (i%schema_for_table.number_of_columns); 
            switch (schema_for_table.data_type[column_no])
            {
            case DBINT:{
                data<int> datum{row_id,std::stoi(data_as_string[i]),0}; // type casting 
                
                
                (*(table_data[column_no].all_data.int_data)).push_back(datum); 
                break;
                }
            case DBCHAR:{
                data<char> datum {row_id,data_as_string[i][0],0};
                (*(table_data[column_no].all_data.char_data)).push_back(datum); 
                break;
                }
            case DBDOUBLE:{
                data<double> datum {row_id,std::stod(data_as_string[i]),0};
                (*(table_data[column_no].all_data.double_data)).push_back(datum); 
                break;
                }
            case DBSTRING:{

                Dbstr string_val;

                data<Dbstr> datum {row_id,/*that string modifing methods go here;*/string_val,0};
                (*(table_data[column_no].all_data.str_data)).push_back(datum);
                break;
                }
            case DBFLOAT:{
                data<float> datum {row_id,std::stof(data_as_string[i]),0};
                (*(table_data[column_no].all_data.float_data)).push_back(datum);
                break;
                }
            }
        
         }
    }

    std::cout<<table_data.size()<<std::endl;

    return table_data;
}    