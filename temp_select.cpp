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




#include <iostream>
#include <vector>
#include <queue>
#include <functional>

template <typename T>
std::vector<int> getTopN(const std::vector<int>& data, int N) {
    // Check if N is larger than the number of elements in data
    if (N <= 0 || data.size() <= N) {
        return data;  // If N is invalid
    }

    
    return result;
}

int main() {
    std::vector<int> data = {3, 10, 1, 5, 8, 12, 7, 9, 2};
    int N = 6;

    std::vector<int> topN = getTopN(data, N);

    std::cout << "Top " << N << " elements: ";
    for (int num : topN) {
        std::cout << num << " ";
    }

    return 0;
}





#endif
