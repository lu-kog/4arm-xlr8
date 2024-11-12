#include "Node.h"



template <typename T>
bool compute(T & given_value , T &row_value, ConditionType op){
        switch (op)
        {
            case EQUALS:
                return row_value == given_value;
            case NOT_EQUALS:
                return row_value != given_value;
            case LESS_THAN:
                return row_value < given_value;
            case GREATER_THAN:
                return row_value > given_value;
            default:
                throw std::invalid_argument("Invalid Operation!");
                break;
        }
    }

RowID_vector FilterNode::mergeAndRemoveDuplicates(const RowID_vector vec1, const RowID_vector vec2) {
    std::set<int> uniqueElements(filtered_left_row->begin(), filtered_left_row->end());
    uniqueElements.insert(filtered_right_row->begin(), filtered_right_row->end());

    delete vec1, vec2; // free memory

    RowID_vector result = new std::vector<int>(uniqueElements.begin(), uniqueElements.end());
    
    return result;
}


RowID_vector validate_and_apply(const std::string &table_name, RowID_vector rows_to_process = nullptr){
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
        RowID_vector filtered_row_id = this->left.get()->execute(table_name);
        return right.get()->execute(table_name, filtered_row_id);

    }else if(this->conditionType == OR)
    {
        RowID_vector filtered_left_rows = this->left.get()->execute(table_name);
        RowID_vector filtered_right_rows = this->right.get()->execute(table_name);

        return mergeAndRemoveDuplicates(filtered_left_rows, filtered_right_rows);

    }else{
        // read column meta and get data type
        // check value's data type and call template function with switch case.
        
        // validate filter
        return validate_and_apply(table_name);
        
    }
    
}

RowID_vector FilterNode::execute(std::string table_name, RowID_vector row_ids){
    /*
    Read data of given row ids
    it can be and, or condtion
    if and, give param left and lefts output to the right
    if or, give the same param for both left and right and put into Set{};
    */

    if (this->conditionType == AND)
    {
        RowID_vector filtered_row_id = this->left.get()->execute(table_name, row_ids);
        return right.get()->execute(table_name, filtered_row_id);

    }else if(this->conditionType == OR)
    {
        RowID_vector filtered_left_row = this->left.get()->execute(table_name, row_ids);
        RowID_vector filtered_right_row = this->right.get()->execute(table_name, row_ids);

        return mergeAndRemoveDuplicates(filtered_left_row, filtered_right_row);

    }else{
        return validate_and_apply(table_name, row_ids);
    }
}

template <typename T>
RowID_vector FilterNode::apply_filter(std::string table_name, RowID_vector rows_to_process = nullptr) {

    
    std::pair<block_meta<T> *, int> meta_array = get_all_block_meta(table_name, this->columnName);

    //  Skip blocks
    std::vector<int> selected_blocks;
    for (size_t i = 0; i < meta_array.second; i++)
    {
        block_meta<T> temp = *meta_array.first[i];
        bool in_range = temp.count > 0 && this->value >= temp.min && this->value <= temp.max;
        if (in_range)
        {
            selected_blocks.push_back(i);
        }
        
    }
    
    // get data for selected blocks
    std::vector<data<T>> * full_data = get_block_data(table_name, this->columnName, selected_blocks, meta_array);

    // skip row Ids
    std::unordered_set<int> filterSet;
    if (rows_to_process)
    {
        filterSet = std::unordered_set<int>(rows_to_process.begin(), rows_to_process.end());
    }
    
    
    RowID_vector result = new std::vector<int>();
    for (int i = 0; i < full_data->size(); i++)
    {
        data<T> &d = full_data->at(i);
        if (rows_to_process==nullptr || filterSet.count(d.row_id))
        {
            if (compute(this->value, d.data, this->conditionType))
            {
                result->push_back(d.row_id);
            }
            
        }
        
    }
    
    // clear memory

    return result;
}

