#include "../include/Column.h"

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

void initilaize_column_objs(const schema_meta &schema, std::vector<column_obj> &table_data,const int &total_data){
    for (size_t i = 0; i < schema.number_of_columns; i++)
    {
        column_obj temp;

        temp.meta.data_type = schema.data_type[i];

        create_vec(temp);  // Just for allocating vector with given data type
        table_data.push_back(temp);
    }
}


column_meta * get_column_meta(const std::string &table_name, const std::string &column_name ){
    int column_meta_size = sizeof(column_meta);
    column_meta *col_met = new column_meta;

    std::string relative_path = table_name +"/"+column_name;
    readBinaryFile(get_path()+relative_path ,(char *) col_met , column_meta_size, 0);

    return col_met;

}

void write_column_meta(const std::string &table_name, const std::string &column_name , column_meta &col_meta_to_write){
    std::string relative_path = table_name +"/"+column_name;
    writeBinaryFile(get_path()+relative_path, (char *) &col_meta_to_write, sizeof(col_meta_to_write) , 0);
}

std::vector<std::string> * get_all_column_names(std::string table_name){
    std::vector<std::string> * column_names = new std::vector<std::string>;
    schema_meta * table_schema = read_schema(table_name);
    for (int i = 0; i < table_schema->number_of_columns; i++)
    {
        column_names->push_back(std::string(table_schema->fields[i].second, table_schema->fields[i].first));
    }
    return column_names;

}




