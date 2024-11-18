#pragma once

#ifndef _META_HANDLER
#define _META_HANDLER 0


#define records_limit 100

#include <vector>
#include "meta.h"
#include <string>
#include "fileHandler.cpp"
#include "cred.cpp"
/*
Get vector of Column objects.
Each object have bunch of data as union of vector * 
create a block of 100 values
calculate meta data
create block object and push to persistence

Handle cases:
    initial insert / continuous

*/

column_meta * get_column_meta(const std::string &table_name, const std::string &column_name ){
    int size = sizeof(column_meta);
    column_meta *col_met = new column_meta;


    std::string relative_path = table_name +"/"+column_name;

    readBinaryFile(get_path()+relative_path ,(char *) col_met , size, 0);


    return col_met;

}


void write_column_meta(const std::string &table_name, const std::string &column_name , column_meta &col_meta_to_write){
    std::string relative_path = table_name +"/"+column_name;

    writeRecords(get_path()+relative_path, (char *) &col_meta_to_write, sizeof(col_meta_to_write) , 0);
}


#endif
