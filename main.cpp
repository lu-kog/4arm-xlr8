#include <iostream>
#include <vector>
#include <string>
#include "cred.cpp"

#include "coverter.cpp"


void print_data(column_obj column);
void print_dt(int data_type);






int main() {

    get_home_folder();






    // create_table("jith",{"Komi", "Najimi", "Tadano"},{1,2,3});

    


    // int option;
#if 1
    std::string tableName = "kcc";
    int numColumns;
    std::vector<std::string> columnNames = {"Komi", "Najimi", "Tadano"};
    std::vector<int> columnDataTypes = {DBINT,DBSTRING,DBFLOAT};



    create_table(tableName,columnNames,columnDataTypes);
#endif


#if 0
    std::vector<std::string> data = {"1","Jith", "6.9","2","Krish", "4.20","69", "Monish", "9.11"};
    schema_meta *tb = read_schema("kcc");

    std::vector <column_obj> col_vec;


    type_casting(data,*tb,col_vec, "kcc");

    // std::cout<<col_vec.size()<<std::endl;


    for (size_t i = 0; i < col_vec.size(); i++)
    {
        
        print_data(col_vec[i]);
    }
    


#endif


#if 0

    schema_meta *tb = read_schema("kcc");


    std::cout<<tb->number_of_columns<<"\n";

    for (size_t i = 0; i < tb->number_of_columns; i++)
    {
        std::cout << tb->fields[i].second<<"\t";
    }

    std::cout << "\n";


    for (size_t i = 0; i < tb->number_of_columns; i++)
    {
        print_dt(tb->data_type[i]);
    }
    
#endif





    return 0;
}



#if 0


void print_data(column_obj column){
    switch (column.meta.data_type)
    {
    case DBINT:
        std::cout<<"Int"<<"\n";
        for (size_t i = 0; i < column.meta.total_records; i++)
        {
            std::cout<<column.all_data.int_data[0][i].data<<std::endl;
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

#endif






void print_dt(int data_type){
    switch (data_type)
    {
    case DBINT:
        std::cout<<"Int"<<"\t";
        break;
    case DBCHAR:
        std::cout<<"Char"<<"\t";
        break;
    case DBLONG:
        std::cout<<"Long"<<"\t";
        break;
    case DBDOUBLE:
        std::cout<<"Double"<<"\t";
        break;
    case DBSTRING:
        std::cout<<"String"<<"\t";
        break;
    case DBFLOAT:
        std::cout<<"Float"<<"\t";
        break;
    
    default:
        break;
    }
}



// void read_table()






