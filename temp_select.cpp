#ifndef SELECT_ 
#define SELECT_ 0

#include<iostream>
// #include "Node.h"
// #include <string>


// void execute_select(QueryNode selectQry){
//     std::string table_name = selectQry.selectNode.tableName;
//     std::vector<int> * row_id = selectQry.filterNode->execute(table_name);
// }

union{
        char c; int i; long l; float f; double d;
    } value; 

int main(int argc, char const *argv[])
{
    value.d = 145.83;

    return 0;
}




#endif
