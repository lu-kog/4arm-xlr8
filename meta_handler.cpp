#pragma once

#ifndef _META_HANDLER
#define _META_HANDLER 0


#include <vector>
#include "meta.h"

/*
Get vector of Column objects.
Each object have bunch of data as union of vector * 
create a block of 100 values
calculate meta data
create block object and push to persistence

Handle cases:
    initial insert / continuous

*/

#include "meta.h"
#include<iostream>

#define records_limit 100

void init_meta(, ){
    /*
    I need data type
    */

    column_meta meta_to_process;


}


void dump_data(std::vector<column_obj> data){

}

int main(int argc, char const *argv[])
{
    std::cout << block_meta_size << std::endl;
    return 0;
}


#endif
