#ifndef SELECT_ 
#define SELECT_ 0

#include<iostream>
#include "Node.h"
#include <string>


void execute_select(QueryNode selectQry){
    std::string table_name = selectQry.selectNode.tableName;
    std::vector<int> * row_id = selectQry.filterNode->execute(table_name);
}




#endif
