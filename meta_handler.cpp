#pragma once

#ifndef _META_HANDLER
#define _META_HANDLER 0

/*
Get data from dumped file
create a block of 100 values
calculate meta data
create block object and push to persistence

Handle cases:
    initial insert / continuous

*/

#include "meta.h"
#include<iostream>

#define records_limit 100

void init_meta(string dump_file){
    /*
    I need data type
    dumped column meta size (column meta)
    */

    column_meta meta_to_process;
    

}

int main(int argc, char const *argv[])
{
    std::cout << block_meta_size << std::endl;
    return 0;
}


#endif
