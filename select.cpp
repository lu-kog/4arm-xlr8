#pragma once

#ifndef _SELECT
#define _SELECT 9;

#include <vector>
#include <string>
#include <algorithm>
#include <iostream>

#include "Node.h"
#include "Node.cpp"
#include "meta.h"
#include "data_fetcher.cpp"
#include "meta_handler.cpp"
#include "fileHandler.cpp"
#include "string_handler.cpp"

struct selected_col_opj{
    int data_type;
    union{
        std::vector<int> *int_data;
        std::vector<float> *float_data;
        std::vector<long> *long_data;
        std::vector<double> *double_data;
        std::vector<std::string> *str_data;
        std::vector<char> *char_data;
    } all_data;

};

template <typename T>
struct comp
{
    SortOrder order;


    bool operator()(data<T> a, data<T> b){
        if (order == ASC)
        {
            return (b.data) > (a.data);   
        }
        return (b.data) < (a.data);
    }
};

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


std::ostream &operator<<(std::ostream & os, Dbstr str){
    return os;
}


template<typename T>
RowID_vector limit_data(const std::string & table_name, const std::string &col_name, LimitNode *l_node = nullptr){
    RowID_vector result = new std::vector<int>();

    std::vector<data<T>> * data_to_limit = get_data_with_rowid<T>(table_name,col_name,nullptr);
    
    for (data<T> &i : (*data_to_limit))
    {
        result->push_back(i.row_id);        
    }


    if ( (l_node != nullptr) && ( l_node->limit < result->size()))
    {
        result->resize(l_node->limit);
    }
    
    delete data_to_limit;

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

RowID_vector execute(const QueryNode &q_node){
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
    if (result == nullptr)
    {
        result = get_limit_data(table_name,columns[0], q_node.limitNode);
    }
    



    return result;
}


template<typename T>
std::vector<T>* get_data_from_column(const std::string &table_name, const std::string &col_name, RowID_vector row_id){

    std::vector<data<T>> * data_from_file = get_data_with_rowid<T>(table_name,col_name,row_id);

    std::vector<T> * data_to_project = new std::vector<T>;

    for (size_t i = 0; i < data_from_file->size(); i++)
    {
        data_to_project->push_back((*data_from_file)[i].data);
    }
    delete data_from_file;
    
    return data_to_project;

}

template<>
std::vector<std::string>* get_data_from_column<std::string>(const std::string &table_name, const std::string &col_name, RowID_vector row_id) {
    std::vector<data<Dbstr>> * data_from_file = get_data_with_rowid<Dbstr>(table_name,col_name,row_id);

    std::vector<std::string> * data_to_project = new std::vector<std::string>;
    for (size_t i = 0; i < data_from_file->size(); i++)
    {
        data_to_project->push_back(str_file_reader(table_name,(*data_from_file)[i].data));
    }
    
    return data_to_project;


}





selected_col_opj get_data_from_column(const std::string &table_name, const std::string &col_name, RowID_vector row_ids){
    column_meta * col_meta = get_column_meta(table_name,col_name);

    selected_col_opj col_obj;
     switch (col_meta->data_type) {
        case DBINT:{
            col_obj.data_type = DBINT;
            col_obj.all_data.int_data = get_data_from_column<int>(table_name,col_name,row_ids);
            break;
        };
        case DBCHAR:{
            col_obj.data_type = DBCHAR;
            col_obj.all_data.char_data = get_data_from_column<char>(table_name,col_name,row_ids);
             break;   
        };

        case DBLONG:{
            col_obj.data_type = DBLONG;
            col_obj.all_data.long_data = get_data_from_column<long>(table_name,col_name,row_ids);
            break;
        };

        case DBDOUBLE:{
            col_obj.data_type = DBDOUBLE;
            col_obj.all_data.double_data = get_data_from_column<double>(table_name,col_name,row_ids);
            break;
        };

        case DBFLOAT:{
            col_obj.data_type = DBFLOAT;
            col_obj.all_data.float_data = get_data_from_column<float>(table_name,col_name,row_ids);
            break;
        };
        case DBSTRING:{
            col_obj.data_type = DBSTRING;
            col_obj.all_data.str_data = get_data_from_column<std::string>(table_name,col_name,row_ids);
            break;
        };
        default:{
            throw std::invalid_argument("Unexpected data type in sort node");
        };
    }
    return col_obj;
}


std::vector<selected_col_opj> get_selected_data(SelectNode select, RowID_vector row_ids){
    std::vector<selected_col_opj> all_data;
    for (size_t i = 0; i < select.columns.size(); i++)
    {
        all_data.push_back(get_data_from_column(select.tableName,select.columns[i], row_ids));
    }
    return all_data;

}


void print_data_at_index(const selected_col_opj& obj, int index) {
    switch (obj.data_type) {
        case DBINT: // int_data
            std::cout<< (*obj.all_data.int_data)[index] << "\t"<<"|";
            break;
        case DBFLOAT: // float_data
            std::cout <<  (*obj.all_data.float_data)[index] << "\t"<<"|";
            break;
        case DBLONG: // long_data
            std::cout  << (*obj.all_data.long_data)[index] << "\t"<<"|";
            break;
        case DBDOUBLE: // double_data
            std::cout  << (*obj.all_data.double_data)[index] << "\t"<<"|";
            break;
        case DBSTRING: // str_data
            std::cout  << (*obj.all_data.str_data)[index] << "\t"<<"|";
            break;
        case DBCHAR: // char_data
            std::cout<< (*obj.all_data.char_data)[index] << "\t"<<"|";
            break;
        default:
            std::cout << "Unknown data type." << std::endl;
    }
}

    

int main(int argc, char const *argv[])
{
    get_home_folder();
    QueryNode qn;

    std::vector<std::string> cols = {"Komi"};
    std::string t_name = "kcc"; 

    qn.selectNode.columns = cols;
    qn.selectNode.tableName = t_name;

    FilterNode left("Tadano",80.1f,GREATER_THAN);

    FilterNode right("Komi",10,GREATER_THAN);

    FilterNode rn(&left,&right,AND);


    // qn.filterNode = &rn;


    SortNode sn("Tadano",DESC);

    // qn.sortNode = &sn;


    RowID_vector res =  execute(qn);


    std::vector<data<int>> * data_to_sort = get_data_with_rowid<int>(t_name,cols[0],res);

    for (auto &&i : *data_to_sort)
    {
        std::cout << i.data << std::endl;
    }
    

    return 0;
}

#endif