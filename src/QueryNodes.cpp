#include "../include/QueryNodes.h"


/*--------------------------------------------------------------------------------------------------*/

// Table creation


void create_table(std::string table_name,std::vector <std::string> &columns_name, std::vector <int> &columns_data_type){

    if(!create_folder(table_name)){  // data directory
        std::cout<<"Table already Exisit\n";
        return;
    };
    
    write_schema(table_name, columns_name, columns_data_type);

    for (size_t i = 0; i < columns_name.size(); i++)
    {
        column_meta meta{0,columns_data_type[i],0};
        write_column_meta(table_name,columns_name[i],meta);
    }
    
}

/*--------------------------------------------------------------------------------------------------*/

// Insert functions


void insertIntoTable(std::vector<column_obj> &columns_to_insert, std::string &table_name, schema_meta &table_schema) {
    backup_table_data(table_name);

    std::vector<std::future<void>> futures;
    std::atomic<bool> gotError(false); // Flag signals for threads

    try
    {
        for (int i = 0; i < table_schema.number_of_columns; i++)
        {
            std::string col_name(table_schema.fields[i].second, table_schema.fields[i].first);
            futures.push_back(std::async(createBlocks, std::ref(columns_to_insert.at(i)), std::ref(table_name), col_name));
        }
        
        for (auto &fut : futures) {
            try
            {
                fut.get();  // Wait for all threads to complete
            }
            catch(const std::exception& e)
            {
                gotError = true;
                std::cout << e.what() << std::endl;
            } 
        }

        if (gotError)
        {
            throw std::runtime_error("Error on insertion");
        }
        
    }
    catch(const std::exception& e)
    {
        Roll_Back(table_name);
        throw;
    }
    
}

#if 0
// Single threaded insertion
void insertIntoTable(std::vector<column_obj> & columns_to_insert, std::string& table_name, schema_meta & table_schema){
    for (int i = 0; i < columns_to_insert.size(); i++)
    {
        std::string col_name(table_schema.fields[i].second, table_schema.fields[i].first); // While creating for thread handle the col_name scope; 
        createBlocks(columns_to_insert.at(i), table_name, col_name);

    }
}
#endif



std::vector<column_obj> &type_casting(std::vector<std::string> &data_as_string, schema_meta &schema_for_table, std::vector<column_obj> &table_data, const std::string &table_name)
{

    int number_of_data = data_as_string.size();

    // to assign row-id, need count of rows from any column
    std::string col_name(schema_for_table.fields[0].second,schema_for_table.fields[0].first);

    column_meta * column_meta = get_column_meta(table_name,col_name);

    int initaial_row_id = column_meta->total_records;


    initilaize_column_objs(schema_for_table,table_data,number_of_data);


    // when the number of columns and given data doesn't match
    //  no column = 3
    //  data = 5 --
    if ((number_of_data % schema_for_table.number_of_columns) != 0)
    {
        throw std::invalid_argument("Data count mismatch!");
    }
    else
    {
        int file_no = 1;  // for string handling
        std::ofstream str_file; 
        get_file_to_write(file_no,table_name, str_file);


        for (size_t i = 0; i < number_of_data; i++)
        {
            unsigned int row_id = initaial_row_id +  (i / schema_for_table.number_of_columns);  // same row-id should repeat for each column
            int column_no = (i % schema_for_table.number_of_columns);
            switch (schema_for_table.data_type[column_no])
            {
            case DBINT:
            {
                data<int> datum{row_id, 0 ,std::stoi(data_as_string[i])}; // type casting

                (*(table_data[column_no].all_data.int_data)).push_back(datum);

                break;
            }
            case DBCHAR:
            {
                data<char> datum{row_id , 0 , data_as_string[i][0]};
                (*(table_data[column_no].all_data.char_data)).push_back(datum);
                break;
            }
            case DBDOUBLE:
            {
                data<double> datum{row_id , 0 , std::stod(data_as_string[i])};
                (*(table_data[column_no].all_data.double_data)).push_back(datum);
                break;
            }
            case DBSTRING:
            {
                Dbstr string_val = str_file_writer(str_file,data_as_string[i],file_no);

                data<Dbstr> datum{row_id , 0 , string_val};
                (*(table_data[column_no].all_data.str_data)).push_back(datum);

                string_write_file_size_check(file_no, table_name, str_file);

                break;
            }
            case DBFLOAT:
            {
                data<float> datum{row_id , 0 , std::stof(data_as_string[i])};
                (*(table_data[column_no].all_data.float_data)).push_back(datum);
                break;
            }
            case DBLONG:
            {

                data<long> datum{row_id , 0 , std::stol(data_as_string[i])};
                (*(table_data[column_no].all_data.long_data)).push_back(datum);
                break;
            }
            }
        }

        str_file.close();
    }   

    return table_data;
}



void insert(std::string table_name, std::string csv_path){
    std::ifstream csvFile(csv_path);

    schema_meta * schema = read_schema(table_name);


    while (!csvFile.eof())
    {
        std::vector<std::string> * parsed = parseCSV(csvFile);
        std::vector<column_obj> table_data; 
        type_casting(*parsed,*schema, table_data,table_name);
        insertIntoTable(table_data,table_name, *schema);

        for (size_t i = 0; i < schema->number_of_columns; i++)
        {
            std::string col_name(schema->fields[i].second,schema->fields[i].first);
            column_meta * col_met = get_column_meta(table_name,col_name);
            delete col_met;
        }
        
    }
    

    delete schema;
}


/*--------------------------------------------------------------------------------------------------*/



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

template <typename T>
bool block_meta_check(FilterNode & f_node,T min, T max)
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

    /*
     * get all blocks meta data, skip if can
     * apply filter condition to all data batch by batch
    */
    
    std::pair<block_meta<T> *, int> meta_array = get_all_block_meta<T>(table_name, this->columnName);

    //  Skip blocks with meta data
    std::vector<int> selected_blocks;
    for (size_t i = 0; i < meta_array.second; i++)
    {
        block_meta<T> temp = meta_array.first[i];
        bool in_range = temp.count > 0 && block_meta_check(*this, temp.min, temp.max);
        if (in_range)
        {
            selected_blocks.push_back(i+1);
        }
        
    }
    
    std::set<int> filterSet;  // for hashtable only
    if (rows_to_process)
    {
        filterSet = std::set<int>(rows_to_process->begin(), rows_to_process->end());
    }


    RowID_vector result = new std::vector<int>();
    
    const int BLOCK_VECTOR_LIMIT = 100;  // 100 blocks data per batch

    int batch_end = 0;
    for (int i = 0; i< selected_blocks.size(); i += batch_end)
    {
        batch_end = std::min(i + BLOCK_VECTOR_LIMIT, static_cast<int>(selected_blocks.size() - i));

        block_meta<T> & current_block_meta = meta_array.first[i];
        auto it = selected_blocks.begin()+i;
        std::vector<int> current_batch(it, it+batch_end);  
        std::vector<data<T>> * current_batch_data = get_block_data<T>(table_name, this->columnName, current_batch, meta_array);

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
                if (compute(filter_value, d.datum, this->conditionType)) {
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

RowID_vector FilterNode::validate_and_apply(const std::string &table_name, RowID_vector rows_to_process){
    /*
     * Just a type caster function
     */

    column_meta * temp_col = get_column_meta(table_name, this->columnName);
    int col_type = temp_col->data_type;
    delete temp_col;

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
     * Read full block meta
     * skip rowID with conditions
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
        // To call Templated function
        return validate_and_apply(table_name, nullptr);
        
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




/*--------------------------------------------------------------------------------------------------*/

// Sort Functions



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
    delete data_to_sort;

    return result;

};

template<typename T>
RowID_vector sort_data(const std::string &table_name,SortNode *sort_n, LimitNode* N, RowID_vector filtered_rows){

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
    if (N != nullptr && ( N->limit < result->size())){
        result->resize(N->limit);
    }

    return result;


}


RowID_vector get_sorted_data(const std::string &table_name,SortNode *sort_n, RowID_vector filtered_rows, LimitNode* N){

    std::string &col_name = sort_n->columnName;
    column_meta * col_meta = get_column_meta(table_name,col_name);



    switch (col_meta->data_type) {
        case DBINT:{
            
            return sort_data<int>(table_name,sort_n, N, filtered_rows);
        };
        case DBCHAR:{
            return sort_data<char>(table_name,sort_n, N, filtered_rows);
        };

        case DBLONG:{
            return sort_data<long>(table_name,sort_n, N, filtered_rows);
        };

        case DBDOUBLE:{
            return sort_data<double>(table_name,sort_n, N, filtered_rows);
        };

        case DBFLOAT:{
            return sort_data<float>(table_name,sort_n, N, filtered_rows);
        };
        default:{
            throw std::invalid_argument("Unexpected data type in sort node");
        };

        
    }
}


/*--------------------------------------------------------------------------------------------------*/

// Limit Functions


template<typename T>
RowID_vector limit_data(const std::string & table_name, const std::string &col_name, LimitNode *l_node){
    RowID_vector result = new std::vector<int>();

    std::vector<data<T>> * data_to_limit = get_data_with_rowid<T>(table_name,col_name,nullptr);  // fetch all rows
    
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


/*--------------------------------------------------------------------------------------------------*/

// Select Query Functions

RowID_vector run_query_nodes(QueryNode &qn){
    const std::string &table_name = qn.selectNode.tableName;
    std::vector<std::string> * columns = &qn.selectNode.columns;

    if (columns->size() == 0)
    {
        qn.selectNode.columns = *get_all_column_names(table_name);
    }
    

    RowID_vector result = nullptr;
    if (qn.filterNode != nullptr)
    {
        result= qn.filterNode->execute(table_name);

        // limit impl have it
        if ( (qn.limitNode != nullptr) && ( qn.limitNode->limit < result->size()))
        {
            result->resize(qn.limitNode->limit);
        }
    }
    if (qn.sortNode)
    {
        result = get_sorted_data(table_name,qn.sortNode,result,qn.limitNode);
    }

    //Only limit node found
    if (result == nullptr)
    {
        result = get_limit_data(table_name,columns->at(0), qn.limitNode);
    }
    
    return result;
}



void execute_select(QueryNode &qn){
    RowID_vector result = run_query_nodes(qn);
    std::vector<selected_col_opj> * sel_data = get_selected_data(qn.selectNode,result);
    print_data_col_obj(*sel_data,result->size());

}


// Projection

template<typename T>
std::vector<T>* get_data_from_column(const std::string &table_name, const std::string &col_name, RowID_vector row_id){

    std::vector<data<T>> * data_from_file = get_data_with_rowid<T>(table_name,col_name,row_id);

    std::vector<T> * data_to_project = new std::vector<T>;

    for (size_t i = 0; i < data_from_file->size(); i++)
    {
        data_to_project->push_back((*data_from_file)[i].datum);
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
        data_to_project->push_back(str_file_reader(table_name,(*data_from_file)[i].datum));
    }
    
    delete data_from_file;
    return data_to_project;
}

selected_col_opj  get_data_as_union(const std::string &table_name, const std::string &col_name, RowID_vector row_ids){
    column_meta * col_meta = get_column_meta(table_name,col_name);
   
    selected_col_opj  col_obj;
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


std::vector<selected_col_opj> * get_selected_data(SelectNode &select, RowID_vector row_ids){
    std::vector<selected_col_opj> * all_data = new std::vector<selected_col_opj>;
    for (size_t i = 0; i < select.columns.size(); i++)
    {
        all_data->push_back(get_data_as_union(select.tableName,select.columns[i], row_ids));
    }
    return all_data;

}

void print_data_col_obj(std::vector<selected_col_opj> &se_d, int number_rows) {
    int number_columns = se_d.size();

    // Step 1: Calculate maximum column widths
    std::vector<int> column_widths(number_columns, 3); // Default minimum width

    for (size_t j = 0; j < number_columns; j++) {
        selected_col_opj &obj = se_d[j];
        for (size_t i = 0; i < number_rows; i++) {
            int len = 0;
            switch (obj.data_type) {
                case DBINT: 
                    len = std::to_string((*obj.all_data.int_data)[i]).size();
                    break;
                case DBFLOAT:
                    len = std::to_string((*obj.all_data.float_data)[i]).size();
                    break;
                case DBLONG:
                    len = std::to_string((*obj.all_data.long_data)[i]).size();
                    break;
                case DBDOUBLE: {
                    std::ostringstream oss;
                    oss << std::fixed << std::setprecision(2) << (*obj.all_data.double_data)[i];
                    len = oss.str().size();
                    break;
                }
                case DBSTRING:
                    len = (*obj.all_data.str_data)[i].size();
                    break;
                case DBCHAR:
                    len = 1; // char is a single character
                    break;
                default:
                    len = 0;
            }
            column_widths[j] = std::max(column_widths[j], len + 2); // Add padding
        }
    }

    // Step 2: Print rows with alignment
    for (size_t i = 0; i < number_rows; i++) {
        std::cout << "\n|";
        for (size_t j = 0; j < number_columns; j++) {
            selected_col_opj &obj = se_d[j];
            std::cout << std::setw(column_widths[j]);
            switch (obj.data_type) {
                case DBINT:
                    std::cout << (*obj.all_data.int_data)[i];
                    break;
                case DBFLOAT:
                    std::cout << (*obj.all_data.float_data)[i];
                    break;
                case DBLONG:
                    std::cout << (*obj.all_data.long_data)[i];
                    break;
                case DBDOUBLE:
                    std::cout << std::fixed << std::setprecision(2) << (*obj.all_data.double_data)[i];
                    break;
                case DBSTRING:
                    std::cout << (*obj.all_data.str_data)[i];
                    break;
                case DBCHAR:
                    std::cout << (*obj.all_data.char_data)[i];
                    break;
                default:
                    std::cout << "Unknown";
            }
            std::cout << "  |";
        }
    }
    std::cout << std::endl << std::endl;
}



/*--------------------------------------------------------------------------------------------------*/

// Delete Functions

void mark_as_deleted(QueryNode &qn, RowID_vector & rows_to_delete){
    // table name, col name, query type
    schema_meta * table = read_schema(qn.selectNode.tableName);
    std::string table_name = qn.selectNode.tableName;

    std::vector<std::future<void>> futures;
    std::atomic<bool> gotError(false); // Flag signals for threads
    try
    {
        for (int i = 0; i < table->number_of_columns; i++)
        {
            std::string col_name(table->fields[i].second, table->fields[i].first);
            futures.push_back(std::async(to_update_records, table_name, col_name, DELELT, table->data_type[i], rows_to_delete, nullptr));
        }
        
        for (auto &fut : futures) {
            try
            {
                fut.get();  // Wait for all threads to complete
            }
            catch(const std::exception& e)
            {
                gotError = true;
            } 
        }

        if (gotError)
        {
            throw std::runtime_error("Error on deletion.");
        }
        
    }
    catch(const std::exception& e)
    {
        Roll_Back(table_name);
        std::cerr << e.what() << '\n';
    }
}


void mark_all_deleted(std::string & table_name){
    /* 
     * read table schema
     * delete whole table folder
     * call create table(old schema)
    */

    schema_meta * table_schema = read_schema(table_name);

    std::vector<std::string> column_names;
    std::vector<int> data_types;
    for (int i = 0; i < table_schema->number_of_columns; i++)
    {
        column_names.push_back(std::string(table_schema->fields[i].second, table_schema->fields[i].first));
        data_types.push_back(table_schema->data_type[i]);
    }

    delete_folder(get_path()+table_name);
    create_table(table_name,column_names,data_types);
}



void execute_delete(QueryNode &qn){
    /* 
    * backup old data
    * apply filter, sort, limit
    * process bunch of blocks
    * if err - roll back
    */
   std::string table_name = qn.selectNode.tableName;
   backup_table_data(table_name); // create backup

   RowID_vector filtered_rows = run_query_nodes(qn);

    column_meta * temp_col = get_column_meta(table_name, qn.selectNode.columns[0]);
    int all_rows = temp_col->total_records;
    delete temp_col;

    if (filtered_rows->size() < all_rows)
    {
        mark_as_deleted(qn, filtered_rows);
    }else{
        mark_all_deleted(table_name);
    }
    
   
   std::cout << "deletion completed" << std::endl;
}


void execute_update(QueryNode &qn){
    /* 
    * backup old data
    * apply filter, sort, limit
    * process bunch of blocks
    * if err - roll back
    */
    std::string table_name = qn.selectNode.tableName;
    backup_table_data(table_name); // create backup

    RowID_vector filtered_rows = run_query_nodes(qn);

    column_meta * temp_col = get_column_meta(table_name, qn.selectNode.columns[0]);
    try
    {
        to_update_records(table_name,qn.selectNode.columns.at(0),UPDATE, temp_col->data_type,filtered_rows, &qn);
    }
    catch(const std::exception& e)
    {
        Roll_Back(table_name);
        std::cerr << e.what() << '\n';
    }
    
    delete temp_col;
   
}




void update_records(std::string table_name, std::string col_name, QueryType qt, RowID_vector rows_to_modify, std::string * new_value){
    Dbstr newStr = insert_new_string(table_name, *new_value);
    update_records<Dbstr>(table_name, col_name, qt, rows_to_modify, &newStr);
}

template<typename T>
void update_records(std::string table_name, std::string col_name, QueryType qt, RowID_vector rows_to_modify, T * new_value)
{
    /*
     * get column meta
     * check rowID vector & sort
     * fetch 100 blocks per batch 
     * check query type
     * modify records for each block
     * dump 100 blocks per a batch to file
    */

    // Step 1: Get column metadata
    auto * col_meta = get_column_meta(table_name, col_name);

    // Step 2: Sort row ID vector if provided
    if (rows_to_modify) {
        std::sort(rows_to_modify->begin(), rows_to_modify->end());
    }else{
        throw std::runtime_error("No data to modify!");
    }


    // Step 3: Fetch 100 blocks per batch
    int chunk_block_no = 1;
    std::vector<block_obj<T>> * data_chunk = get_blocks_chunk<T>(table_name, col_name, chunk_block_no, *col_meta);


    for (int &i : *rows_to_modify)
    {   
        int current_block_no = (i/RECORDS_LIMIT)+1;
        if (current_block_no >= (chunk_block_no + data_chunk->size()))
        {
            // dump previous batch data
            dump_blocks_chunk<T>(table_name, col_name, *data_chunk, chunk_block_no);
            // fetch next batch data
            data_chunk = get_blocks_chunk<T>(table_name, col_name, current_block_no, *col_meta);
            // modify chunk block no
            chunk_block_no = current_block_no;
        }

        // Step 4: Check query type
        data<T> & row = data_chunk->at(current_block_no - chunk_block_no).all_data.at(i%RECORDS_LIMIT);

        if(qt == DELELT){
            row.is_deleted = true;
        }else if(qt == UPDATE){
            if (new_value)
            {
               row.datum = *new_value;
                
            }else{
                throw std::runtime_error("Error: Updation without new value!");
            }
        }

        // block meta updation??

    }
    
    dump_blocks_chunk<T>(table_name, col_name, *data_chunk, chunk_block_no);

    delete data_chunk;
    delete col_meta;
}



void to_update_records(std::string table_name, std::string col_name, QueryType qt, int data_type, RowID_vector rows_to_modify, QueryNode * qn){
    if (qt == UPDATE && qn == nullptr)
        throw std::invalid_argument("Need Query node to update values.");
    
    bool isUpdate = ((qt == UPDATE) && qn->isUpdate);  // to confirm qn is not a nullptr.

    switch (data_type)
    {
    case DBCHAR:
        if (isUpdate)
            update_records(table_name, col_name, qt, rows_to_modify, &std::get<char>(qn->value_to_update));
        else
            update_records<char>(table_name, col_name, qt, rows_to_modify,nullptr);
        break;
    case DBDOUBLE:
        if (isUpdate)
            update_records(table_name, col_name, qt, rows_to_modify, &std::get<double>(qn->value_to_update));
        else
            update_records<double>(table_name, col_name, qt, rows_to_modify,nullptr);
        break;
    case DBINT:
        if (isUpdate)
            update_records(table_name, col_name, qt, rows_to_modify, &std::get<int>(qn->value_to_update));
        else
            update_records<int>(table_name, col_name, qt, rows_to_modify,nullptr);
        break;
    case DBFLOAT:
        if (isUpdate)
            update_records(table_name, col_name, qt, rows_to_modify, &std::get<float>(qn->value_to_update));
        else
            update_records<float>(table_name, col_name, qt, rows_to_modify,nullptr);
        break;
    case DBLONG:
        if (isUpdate)
            update_records(table_name, col_name, qt, rows_to_modify, &std::get<long>(qn->value_to_update));
        else
            update_records<long>(table_name, col_name, qt, rows_to_modify,nullptr);
        break;
    case DBSTRING:
        if (isUpdate)
            update_records(table_name, col_name, qt, rows_to_modify, &std::get<std::string>(qn->value_to_update));
        else
            update_records<Dbstr>(table_name, col_name, qt, rows_to_modify,nullptr);
        break;
    default:
        throw std::invalid_argument("Unexpected datatype");
        break;
    }
}


/*--------------------------------------------------------------------------------------------------*/
