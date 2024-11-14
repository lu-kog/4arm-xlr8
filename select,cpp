#pragma once

#ifndef _SELECT
#define _SELECT 9;

#include <vector>
#include <string>
#include <algorithm>
#include <iostream>

#include "Node.h"
#include "meta.h"
#include "data_fetcher.cpp"
#include "meta_handler.cpp"
#include "fileHandler.cpp"


template <typename T>
struct comp
{
    SortOrder order;


    bool operator()(data<T> a, data<T> b){
        if (order == ASC)
        {
            return b.data > a.data;   
        }
        return b.data < a.data;
    }
};



void *get_data_from_table(const std::string &t_name, const std::string &col_name, std::vector<int> &row_id) {


    


    auto col_meta = get_column_meta(t_name,col_name);



    switch (col_meta->data_type) {
        case DBINT:{
            auto full_col_meta = get_all_block_meta<int>(t_name,col_name);
            
            break;
        };
        case DBCHAR:{
            auto full_col_meta = get_all_block_meta<int>(t_name,col_name);
            
            break;
        };

        case DBLONG:{
            auto full_col_meta = get_all_block_meta<int>(t_name,col_name);
            
            break;
        };

        case DBDOUBLE:{
            auto full_col_meta = get_all_block_meta<int>(t_name,col_name);
            
            break;
        };

        case DBSTRING:{
            auto full_col_meta = get_all_block_meta<int>(t_name,col_name);
            
            break;
        };

        case DBFLOAT:{
            auto full_col_meta = get_all_block_meta<int>(t_name,col_name);
            
            break;
        };

        default:{
            auto full_col_meta = get_all_block_meta<int>(t_name,col_name);
            
            break;
        };
    }

    return nullptr;
}



/* 
 * Converts a vector of integers into ranges. 
 * Consecutive numbers are grouped as "start-end", non-consecutive numbers are individual ranges.
 * Example: {1,2,3,5,6,7,8,10,11,12,13,16} -> {1-3, 5-8, 10-13, 16}
 */

std::vector<std::pair<int, int>> convertToRanges(const std::vector<int>& nums) {
    std::vector<std::pair<int, int>> ranges;
    if (nums.empty()) return ranges;

    int start = nums[0];
    int end = nums[0];

    for (size_t i = 1; i < nums.size(); ++i) {
        if (nums[i] == nums[i - 1] + 1) {
            
            end = nums[i];  
        } else {
            
            ranges.push_back({start, end});
            start = end = nums[i];  
        }
    }

    ranges.push_back({start, end});
    
    return ranges;
}



template <typename T>
void read_data_with_row_id_range(std::ifstream &file,const std::pair<int,int> &range, char* buffer){

    int size;

    int block_no = (range.first/records_limit);
    int data_count;

    int out_of_record_limit = 0;

    size = range.first - range.second + 1;

    if (block_no != range.second/records_limit)
    {
        out_of_record_limit  = (block_no+1) * 100;
    }

    int offset = sizeof(column_meta) + ((sizeof(block_meta<T>) + (sizeof(data<T>) * 100)) * block_no)   + sizeof(block_meta<T>);
    readBinaryFile(buffer,size, offset,file);
}





//Top n sort
template<typename T>
RowID_vector topN_sort_data(std::vector<data<T>> * data_to_sort, comp<T> compare,LimitNode *N){

    std::vector<data<T>> top_slots;


    //Limit + 1 slots for push new val and pop
    top_slots.reserve(N->limit+1);
    

     struct linked_node
    {   
        data<T> val;
        linked_node *left = nullptr;
        linked_node *right = nullptr;
        linked_node(const data<T> &value):val(value){}
        linked_node(){}

        ~linked_node(){
            delete right;
        }

    };


    linked_node * root_node = new linked_node();


    // sorting the first N values
    std::sort(data_to_sort->begin(), data_to_sort->begin()+N->limit, compare);
    

    linked_node * curr_node = root_node;


    // Geting the first n value from input data to linked list
    // 
    for (size_t i = 0; i < N->limit; i++)
    {
        curr_node->val = (*data_to_sort)[i];
        curr_node->right = new linked_node;
        linked_node *prv_node = curr_node;
        curr_node = curr_node->right;
        curr_node->left = prv_node;
    }
    
    curr_node->left->right = nullptr;
    
    linked_node * last_node = curr_node->left;
    delete curr_node;


    for (size_t i = N->limit; i < data_to_sort->size(); i++)
    {
        curr_node = root_node;
        while (curr_node)
        {
            if (compare((*data_to_sort)[i],curr_node->val))
            {


                // if the value is to be swaped
                linked_node *new_node = new linked_node((*data_to_sort)[i]);

                bool is_first_node = (curr_node ==  root_node);
                if (is_first_node)
                {
                    new_node->left = nullptr;  // It's the left most node
                    curr_node->left = new_node;
                    new_node->right = curr_node;


                    root_node = new_node;
                }
                else{
                    curr_node->left->right = new_node;
                    new_node->left = curr_node->left;
                    curr_node->left = new_node;
                    new_node->right = curr_node;
                }
                //poping the last node;
                linked_node * tmp = last_node->left;
                tmp->right = nullptr;

                delete last_node;

                last_node = tmp;


            }
            curr_node = curr_node->right;

                       
        }
        
    }


    curr_node = root_node;

    RowID_vector result = new std::vector<int>();

    while (curr_node)
    {
        result->push_back(curr_node->val.row_id);


        curr_node =  curr_node->right;
    }
    

    delete root_node;
    

    return result;

    

};




template<typename T>
RowID_vector sort_data(const std::string &table_name,SortNode *sort_n, RowID_vector filtered_rows = nullptr, LimitNode* N = nullptr){
    





    std::vector<data<T>> * data_to_sort = get_data_with_rowid<T>(table_name,sort_n->columnName,filtered_rows);


    comp<T> compare;
    compare.order = sort_n->sortOrder;

    if (N != nullptr)
    {

        //topN decision
        if ( N->limit <= data_to_sort->size()/2 )
        {
            delete filtered_rows;
            return topN_sort_data<T>(data_to_sort,compare,N);
        }
        

        
    }
    
    


    std::sort(data_to_sort->begin(), data_to_sort->end(),compare);

    RowID_vector result = new std::vector<int>();

    for (data<T> &i : (*data_to_sort))
    {
        result->push_back(i.row_id);
    }

    //Limit Impl
    if (N != nullptr){
        result->resize(N->limit);
    }


    
    return result;


    
}


template<typename T>
RowID_vector sort_data(const std::string &table_name,SortNode *sort_n, LimitNode* N = nullptr){
    return sort_data<T>(table_name,sort_n,nullptr, N);
}



template<typename T>
RowID_vector limit_data(const std::string & table_name, const std::string &col_name, LimitNode *l_node){
    RowID_vector result = new std::vector<int>();

    std::vector<data<T>> * data_to_sort = get_data_with_rowid<T>(table_name,col_name,nullptr);

    for (data<T> &i : (*data_to_sort))
    {
        result->push_back(i.row_id);
    }

    if (l_node.limit < result->size())
    {
        result->resize(l_node.limit);
    }
    
    delete data_to_sort;

    return result;
}

RowID_vector get_limit_data(const std::string & table_name, const std::string &col_name, LimitNode *l_node){
    column_meta * col_meta = get_column_meta(table_name,col_name);



    switch (col_meta->data_type) {
        case DBINT:{
            
            return limit_data<int>(table_name,col_name,l_node);
        };
        case DBCHAR:{
            return limit_data<char>(table_name,col_name,l_node);
        };

        case DBLONG:{
            return limit_data<long>(table_name,col_name,l_node);
        };

        case DBDOUBLE:{
            return limit_data<double>(table_name,col_name,l_node);
        };

        case DBFLOAT:{
            return limit_data<float>(table_name,col_name,l_node);
        };
        case DBSTRING:{
            return limit_data<Dbstr>(table_name,col_name,l_node);
        };
        default:{
            throw std::invalid_argument("Unexpected data type in sort node");
        };

        
    }
}



RowID_vector get_sorted_data(const std::string &table_name,SortNode *sort_n, RowID_vector filtered_rows = nullptr, LimitNode* N = nullptr){

    std::string &col_name = sort_n->columnName;
    column_meta * col_meta = get_column_meta(table_name,col_name);



    switch (col_meta->data_type) {
        case DBINT:{
            
            return sort_data<int>(table_name,sort_n,filtered_rows,N);
        };
        case DBCHAR:{
            return sort_data<char>(table_name,sort_n,filtered_rows,N);
        };

        case DBLONG:{
            return sort_data<long>(table_name,sort_n,filtered_rows,N);
        };

        case DBDOUBLE:{
            return sort_data<double>(table_name,sort_n,filtered_rows,N);
        };

        case DBFLOAT:{
            return sort_data<float>(table_name,sort_n,filtered_rows,N);
        };
        default:{
            throw std::invalid_argument("Unexpected data type in sort node");
        };

        
    }
}



void* execute(const QueryNode &q_node){
    const std::string &table_name = q_node.selectNode.tableName;
    const std::vector<std::string> & columns = q_node.selectNode.columns;

    RowID_vector result = nullptr;
    if (q_node.filterNode != nullptr)
    {
        result= q_node.filterNode->execute(table_name);
    }
    if (q_node.sortNode)
    {
        result = get_sorted_data(table_name,q_node.sortNode,result,q_node.limitNode);
    }

    //Only limit node found
    if (result == nullptr && q_node.limitNode != nullptr)
    {
        
    }
    
    
    



    return nullptr;
}



#endif