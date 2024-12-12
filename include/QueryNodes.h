#pragma once

#ifndef _NODE_H
#define _NODE_H 0

#include <vector>
#include <string>
#include <cstring>
#include <thread>
#include <atomic>
#include<variant>
#include <iomanip> // For std::setw
#include "Utilities.h"
#include "DbString.h"
#include "Schema.h"
#include "Meta.h"
#include "Column.h"
#include "DataBlock.h"
// #include "../src/csv_parser.cpp"
std::vector<std::string> * parseCSV(std::ifstream &file);

enum ConditionType { EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, OR, AND };

struct selected_col_opj{
    int data_type = 0;
    union{
        std::vector<int> *int_data;
        std::vector<float> *float_data;
        std::vector<long> *long_data;
        std::vector<double> *double_data;
        std::vector<std::string> *str_data;
        std::vector<char> *char_data;
    } all_data;

    void clear() {
        switch (data_type) {
            case DBINT: delete all_data.int_data; break;
            case DBFLOAT: delete all_data.float_data; break;
            case DBLONG: delete all_data.long_data; break;
            case DBDOUBLE: delete all_data.double_data; break;
            case DBSTRING: delete all_data.str_data; break;
            case DBCHAR: delete all_data.char_data; break;
        }
        all_data.int_data = nullptr;
        data_type = 0;
    }

    selected_col_opj(){}

    selected_col_opj &operator=(const selected_col_opj &other) {
        if (this != &other) {
            clear(); // Free existing data
            data_type = other.data_type;
            switch (other.data_type) {
                case DBINT: all_data.int_data = new std::vector<int>(*other.all_data.int_data); break;
                case DBFLOAT: all_data.float_data = new std::vector<float>(*other.all_data.float_data); break;
                case DBLONG: all_data.long_data = new std::vector<long>(*other.all_data.long_data); break;
                case DBDOUBLE: all_data.double_data = new std::vector<double>(*other.all_data.double_data); break;
                case DBSTRING: all_data.str_data = new std::vector<std::string>(*other.all_data.str_data); break;
                case DBCHAR: all_data.char_data = new std::vector<char>(*other.all_data.char_data); break;
                default: all_data.int_data = nullptr; break;
            }
        }
        return *this;
    }

    // Copy constructor
    selected_col_opj(const selected_col_opj &other) : data_type(other.data_type) {
        switch (other.data_type) {
            case DBINT: all_data.int_data = new std::vector<int>(*other.all_data.int_data); break;
            case DBFLOAT: all_data.float_data = new std::vector<float>(*other.all_data.float_data); break;
            case DBLONG: all_data.long_data = new std::vector<long>(*other.all_data.long_data); break;
            case DBDOUBLE: all_data.double_data = new std::vector<double>(*other.all_data.double_data); break;
            case DBSTRING: all_data.str_data = new std::vector<std::string>(*other.all_data.str_data); break;
            case DBCHAR: all_data.char_data = new std::vector<char>(*other.all_data.char_data); break;
            default: all_data.int_data = nullptr; break; 
        }
    }

    

    ~selected_col_opj() {
        clear();
    }

};

struct SelectNode {
    std::vector<std::string> columns;
    std::string tableName;
};


struct FilterNode {
    std::string columnName; // must be int column
    union{
        char c; int i; long l; float f; double d;
    } value;  
    int data_type;
    ConditionType conditionType;
    FilterNode * left;
    FilterNode * right;

    // Overloaded Constructors for simple conditions
    FilterNode(const std::string& col, char val, ConditionType cond)
        : columnName(col), data_type(DBCHAR), conditionType(cond), left(nullptr), right(nullptr) { value.c = val; }
    FilterNode(const std::string& col, int val, ConditionType cond)
        : columnName(col), data_type(DBINT), conditionType(cond), left(nullptr), right(nullptr) { value.i = val; }
    FilterNode(const std::string& col, long val, ConditionType cond)
        : columnName(col), data_type(DBLONG), conditionType(cond), left(nullptr), right(nullptr) { value.l = val; }
    FilterNode(const std::string& col, float val, ConditionType cond)
        : columnName(col), data_type(DBFLOAT), conditionType(cond), left(nullptr), right(nullptr) { value.f = val; }
    FilterNode(const std::string& col, double val, ConditionType cond)
        : columnName(col), data_type(DBDOUBLE), conditionType(cond), left(nullptr), right(nullptr) { value.d = val; }


    // Constructor for compound conditions
    FilterNode(FilterNode * lhs, FilterNode * rhs, ConditionType cond)
        : left(lhs), right(rhs), conditionType(cond) {}

    void print(){
        if (left) left->print();
        std::cout << "---\nCol Name: " << columnName << "\n" << "\n" << "Condition: " << conditionType << "\n---\n";
        if (right) right->print();
    }


    RowID_vector validate_and_apply(const std::string &table_name, RowID_vector rows_to_process = nullptr);
    template <typename T>
    RowID_vector apply_filter(const std::string& table_name, RowID_vector rows_to_process);
    RowID_vector execute(const std::string& table_name);
    RowID_vector execute(const std::string& table_name, RowID_vector row_ids);


};

// Filter helper functions
template <typename T>
bool compute(const T & filter_value , const T &row_value, ConditionType op);
template <typename T>
bool block_meta_check(FilterNode & f_node,T min, T max);




enum SortOrder { ASC, DESC };

template <typename T>
struct comp
{
    SortOrder order;


    bool operator()(data<T> a, data<T> b){
        if (order == ASC)
        {
            return (b.datum) > (a.datum);   
        }
        return (b.datum) < (a.datum);
    }
};

struct SortNode {
    std::string columnName;
    SortOrder sortOrder;

    SortNode(const std::string &col, SortOrder order = ASC){
        columnName = col;
        sortOrder = order;
    }

    void print(){
        std::cout << "Sort Column:" << columnName << std::endl;
        std::cout << "Order: " << sortOrder << std::endl;
    }
};


struct LimitNode
{
    int limit;
    LimitNode(int limit_){
        limit = limit_;
    }

    LimitNode(){};

    void print(){
        std::cout << "Limits: " << limit << std::endl;
    }
};


// Sort Helper Functions

template<typename T>
RowID_vector topN_sort_data(std::vector<data<T>> * data_to_sort, comp<T> compare,LimitNode *N);

template<typename T>
RowID_vector sort_data(const std::string &table_name,SortNode *sort_n, LimitNode* N = nullptr, RowID_vector filtered_rows = nullptr);

RowID_vector get_sorted_data(const std::string &table_name,SortNode *sort_n, RowID_vector filtered_rows = nullptr, LimitNode* N = nullptr);


// Limit Helper Functions
template<typename T>
RowID_vector limit_data(const std::string & table_name, const std::string &col_name, LimitNode *l_node = nullptr);

enum QueryType {DELELT, UPDATE};

struct QueryNode {
    SelectNode selectNode;
    FilterNode * filterNode = nullptr;
    SortNode * sortNode = nullptr;
    LimitNode * limitNode = nullptr;

    bool isSelect = false, isUpdate = false, isDelete = false;
    std::variant<char, int, long, float, double, std::string> value_to_update;

};


void create_table(std::string table_name,std::vector <std::string> &columns_name, std::vector <int> &columns_data_type);

void insert(std::string table_name, std::string csv_path);
std::vector<column_obj> &type_casting(std::vector<std::string> &data_as_string, schema_meta &schema_for_table, std::vector<column_obj> &table_data, const std::string &table_name);
void insertIntoTable(std::vector<column_obj> &columns_to_insert, std::string &table_name, schema_meta &table_schema);

void execute_select(QueryNode &qn);
RowID_vector run_query_nodes(QueryNode &qn);

// projection
std::vector<selected_col_opj> * get_selected_data(SelectNode &select, RowID_vector row_ids);
selected_col_opj get_data_as_union(const std::string &table_name, const std::string &col_name, RowID_vector row_ids);
template<typename T>
std::vector<T>* get_data_from_column(const std::string &table_name, const std::string &col_name, RowID_vector row_id);
template<>
std::vector<std::string>* get_data_from_column<std::string>(const std::string &table_name, const std::string &col_name, RowID_vector row_id);
void print_data_col_obj(std::vector<selected_col_opj> &se_d, int number_rows);


template<typename T>
void update_records(std::string table_name, std::string col_name, QueryType qt, RowID_vector rows_to_modify, T * new_value = nullptr);
void to_update_records(std::string table_name, std::string col_name, QueryType qt, int data_type, RowID_vector rows_to_modify, QueryNode * qn = nullptr);
void mark_as_deleted(QueryNode &qn, RowID_vector & rows_to_delete);
void mark_all_deleted(std::string & table_name);
void execute_delete(QueryNode &qn);
void execute_update(QueryNode &qn);

#endif