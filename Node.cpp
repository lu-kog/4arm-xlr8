#pragma once

#ifndef _NODE_CPP
#define _NODE_CPP 420

#include "Node.h"
#include "meta_handler.cpp"
#include "data_fetcher.cpp"

template <typename T>
bool compute(const T & filter_value , const T &row_value, ConditionType op){
        switch (op)
        {
            case EQUALS:
                return row_value == filter_value;
            case NOT_EQUALS:
                return row_value != filter_value;
            case LESS_THAN:
                return row_value < filter_value;
            case GREATER_THAN:
                return row_value > filter_value;
            default:
                throw std::invalid_argument("Invalid Operation!");
                break;
        }
    }

RowID_vector FilterNode::mergeAndRemoveDuplicates(RowID_vector filtered_left_row, RowID_vector filtered_right_row) {
    std::set<int> uniqueElements(filtered_left_row->begin(), filtered_left_row->end());
    uniqueElements.insert(filtered_right_row->begin(), filtered_right_row->end());

    delete filtered_left_row, filtered_right_row; // free memory

    RowID_vector result = new std::vector<int>(uniqueElements.begin(), uniqueElements.end());
    
    return result;
}


RowID_vector FilterNode::validate_and_apply(const std::string &table_name, RowID_vector rows_to_process = nullptr){
    int col_type = get_column_meta(table_name, this->columnName)->data_type;

    if (this->data_type == DBSTRING || col_type != this->data_type)
    {
        throw std::invalid_argument("Invalid type for the conditions!");
    }

    switch (this->data_type)
    {
    case DBCHAR:
        return apply_filter<char>(table_name, rows_to_process);
        break;
    case DBINT:
        return apply_filter<int>(table_name, rows_to_process);
        break;
    case DBLONG:
        return apply_filter<long>(table_name, rows_to_process);
        break;
    case DBFLOAT:
        return apply_filter<float>(table_name, rows_to_process);
        break;
    case DBDOUBLE:
        return apply_filter<double>(table_name, rows_to_process);
        break;
    
    default:
        throw std::invalid_argument("Invalid type for the filter!");
        break;
    }
}

RowID_vector FilterNode::execute(const std::string &table_name){
    /*
    Read full block meta
    skip with conditions
    */
    if (this->conditionType == AND)
    {
        RowID_vector filtered_row_id = this->left->execute(table_name);
        return right->execute(table_name, filtered_row_id);

    }else if(this->conditionType == OR)
    {
        RowID_vector filtered_left_rows = this->left->execute(table_name);
        RowID_vector filtered_right_rows = this->right->execute(table_name);

        return mergeAndRemoveDuplicates(filtered_left_rows, filtered_right_rows);

    }else{
        // read column meta and get data type
        // check value's data type and call template function with switch case.
        
        // validate filter
        return validate_and_apply(table_name);
        
    }
    
}

RowID_vector FilterNode::execute(const std::string& table_name, RowID_vector row_ids){
    /*
    Read data of given row ids
    it can be and, or condtion
    if and, give param left and lefts output to the right
    if or, give the same param for both left and right and put into Set{};
    */
    RowID_vector result;
    if (this->conditionType == AND)
    {
        RowID_vector filtered_row_id = this->left->execute(table_name, row_ids);
        result = right->execute(table_name, filtered_row_id);

    }else if(this->conditionType == OR)
    {
        RowID_vector filtered_left_row = this->left->execute(table_name, row_ids);
        RowID_vector filtered_right_row = this->right->execute(table_name, row_ids);
        result = mergeAndRemoveDuplicates(filtered_left_row, filtered_right_row);

    }else{
        result = validate_and_apply(table_name, row_ids);
    }

    delete row_ids;
    return result;
}

template <typename T>
bool meta_check(FilterNode & f_node,T min, T max)
{   

    switch (f_node.data_type)
    {
    case DBCHAR:
        return (f_node.value.c >= min && f_node.value.c <= max);
        break;
    case DBDOUBLE:
        return (f_node.value.d >= min && f_node.value.d <= max);
        break;
    case DBINT:
        return (f_node.value.i >= min && f_node.value.i <= max);
        break;
    case DBFLOAT:
        return (f_node.value.f >= min && f_node.value.f <= max);
        break;
    case DBLONG:
        return (f_node.value.l >= min && f_node.value.l <= max);
        break;
    default:
        throw std::invalid_argument("Unexpected datatype");
        break;
    }

}


template <typename T>
RowID_vector FilterNode::apply_filter(const std::string& table_name, RowID_vector rows_to_process) {

    
    std::pair<block_meta<T> *, int> meta_array = get_all_block_meta<T>(table_name, this->columnName);

    //  Skip blocks with meta data
    std::vector<int> selected_blocks;
    for (size_t i = 0; i < meta_array.second; i++)
    {
        block_meta<T> temp = meta_array.first[i];
        bool in_range = temp.count > 0 && meta_check(*this, temp.min, temp.max);
        if (in_range)
        {
            selected_blocks.push_back(i+1);
        }
        
    }
    
    std::unordered_set<int> filterSet;  // for hashtable only
    if (rows_to_process)
    {
        filterSet = std::unordered_set<int>(rows_to_process->begin(), rows_to_process->end());
    }


    RowID_vector result = new std::vector<int>();
    
    const int VECTORIZE_LIMIT = 100;  // 100 blocks data at a time


    int batch_end = 0;
    for (int i = 0; i< selected_blocks.size(); i += batch_end)
    {
        batch_end = std::min(i + VECTORIZE_LIMIT, static_cast<int>(selected_blocks.size() - i));

        block_meta<T> & current_block_meta = meta_array.first[i];
        auto it = selected_blocks.begin()+i;
        std::vector<int> current_batch(it, it+batch_end);  
        std::vector<data<T>> * current_batch_data = get_block_data(table_name, this->columnName, current_batch, meta_array);

        std::vector<int> filtered_ids;
        filtered_ids.reserve(current_batch_data->size());

        T filter_value;
        switch (this->data_type) {
            case DBCHAR:
                filter_value = this->value.c;
                break;
            case DBINT:
                filter_value = this->value.i;
                break;
            case DBLONG:
                filter_value = this->value.l;
                break;
            case DBFLOAT:
                filter_value = this->value.f;
                break;
            case DBDOUBLE:
                filter_value = this->value.d;
                break;
        }

        for (const data<T>& d : *current_batch_data) {
            // Apply row filtering if applicable
            if (!rows_to_process || filterSet.count(d.row_id)) {
                // Apply condition check
                if (compute(filter_value, d.data, this->conditionType)) {
                    filtered_ids.push_back(d.row_id);
                }
            }
        }

        // Append the filtered row IDs to the result
        result->insert(result->end(), filtered_ids.begin(), filtered_ids.end());

        delete current_batch_data;

        
        
    }
    

    return result;
}



#endif