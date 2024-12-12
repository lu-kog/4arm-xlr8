#include "../include/DataBlock.h"
#if 1

void createBlocks(column_obj& clm, std::string& table_name, std::string col_name){

    column_meta meta_to_process = clm.meta;

    int data_type = clm.meta.data_type;
    
    switch (data_type) {
        case DBINT:
            processColumnData<int>(*clm.all_data.int_data, table_name, col_name);
            break;
            
        case DBCHAR:
            processColumnData<char>(*clm.all_data.char_data, table_name, col_name);
            break;
        case DBLONG:
            processColumnData<long>(*clm.all_data.long_data, table_name, col_name);
            break;
        case DBDOUBLE:
            processColumnData<double>(*clm.all_data.double_data, table_name, col_name);
            break;
        case DBSTRING:
            processColumnData<Dbstr>(*clm.all_data.str_data, table_name, col_name);
            break;
        case DBFLOAT:
            processColumnData<float>(*clm.all_data.float_data, table_name, col_name);
            break;
        default:
            std::cerr << "Unsupported data type" << std::endl;
            break;
    }
    
    

}


template <typename T>
block_obj<T> * readIncompleteBlockFromFile(const column_meta& col_meta, std::string& table_name, std::string& col_name) {
    /* Get path & read from file */
    std::string file_path = get_path() + table_name + "/" + col_name;
    int offset = sizeof(column_meta) + ((col_meta.no_block - 1) * sizeof(block_meta<T>)) + ((col_meta.no_block - 1) * RECORDS_LIMIT * sizeof(data<T>));
    

    block_obj<T> *lastBlock = new block_obj<T>();

    // read last block meta
    readBinaryFile(file_path, (char*) (&lastBlock->meta), sizeof(block_meta<T>), offset);

    std::vector<data<T>> tempData(lastBlock->meta.count); // allocate for 100 entries (per block)
    
    // read from the point next to the block meta to count of entries.
    readBinaryFile(file_path, (char*) (tempData.data()), lastBlock->meta.count * sizeof(data<T>), offset + sizeof(block_meta<T>));

    lastBlock->all_data = std::move(tempData);

    return lastBlock;
}

template <typename T>
void processColumnData(std::vector<data<T>> &newRecords, std::string& table_name, std::string& col_name) {
    
    column_meta * col_meta = get_column_meta(table_name, col_name);

    std::vector<block_obj<T>> new_blocks;

    auto it = newRecords.begin();

    bool is_string = (typeid(T) == typeid(Dbstr));
    
    // If unfinished block
    if (col_meta->total_records % RECORDS_LIMIT != 0) {
        block_obj<T> * lastBlock = readIncompleteBlockFromFile<T>(*col_meta, table_name, col_name);

        while (lastBlock->all_data.size() < RECORDS_LIMIT && it != newRecords.end()) {
            lastBlock->all_data.push_back(*it);

            // Meta updation.. skip if it string
            if (!is_string)
            {
                if ((it->datum) > lastBlock->meta.max)
                {
                    lastBlock->meta.max = it->datum;
                }else if ((it->datum) < lastBlock->meta.min)
                {
                    lastBlock->meta.min = it->datum;
                }
            }
            it++;
            
        }

        // update last block meta
        lastBlock->meta.count = lastBlock->all_data.size();
        col_meta->no_block -=1;  // decreasing this block because, should not consider this block in offset calc.
        
        new_blocks.push_back(*lastBlock);
        delete lastBlock;
    }


    auto end = newRecords.end();




    // Process remaining records in blocks of 100
    while (it != end) {
        block_obj<T> block;
        for (int i = 0; i < RECORDS_LIMIT && it != newRecords.end(); i++) {

            block.all_data.push_back(*it);
            
            // Meta calculation.. skip if it string
            if (!is_string){
                if ((it->datum) > block.meta.max)
                {
                    block.meta.max = it->datum;
                }
                if ((it->datum) < block.meta.min)
                {
                    block.meta.min = it->datum;
                }
            }
            it++;
        }

        block.meta.count = block.all_data.size();

        new_blocks.push_back(block);

    }
    
    int records_size = RECORDS_LIMIT * sizeof(data<T>);
    int block_meta_size = sizeof(block_meta<T>);

    // file offset should be starting point of the block.
    // if pending block, offset is it's starting point
    // if the column already have some complete blocks, pointer should be next to all the blocks.

    int file_offset = sizeof(column_meta) + (col_meta->no_block * block_meta_size) + (col_meta->no_block * records_size);


    if (col_meta->no_block == 0)
    {
        file_offset = sizeof(column_meta);
    }
    

    col_meta->total_records += newRecords.size();
    col_meta->no_block += new_blocks.size();


    /*
        overwrite column_meta
        overwrite last block & dump newBlocks
        clear memory()
    */
    
   
   write_column_meta(table_name, col_name, *col_meta);
   dump_new_records(new_blocks, table_name, col_name, file_offset);

    delete col_meta;
    delete &newRecords; // don't know will it work
}

template <typename T>
void dump_new_records(std::vector<block_obj<T>> & new_blocks, std::string &table_name, std::string &col_name, int file_offset){
    int blocks_count = new_blocks.size();
    int block_meta_size = sizeof(block_meta<T>);
    int block_data_size = RECORDS_LIMIT * sizeof(data<T>);

    int buffer_size = (blocks_count * block_meta_size) + (blocks_count * block_data_size);
    char * buffer =  (char *) malloc(buffer_size);
    int offset = 0;


    for (block_obj<T> blk : new_blocks)
    {
        memcpy(buffer+offset, &blk.meta, block_meta_size);
        offset += block_meta_size;
        memcpy(buffer+offset, blk.all_data.data(), blk.all_data.size() * sizeof(data<T>)); // case: unfinished block
        offset += blk.all_data.size() * sizeof(data<T>);
    }
    
    std::string fileName = get_path() + table_name + "/"+col_name;
    
    writeBinaryFile(fileName,buffer, buffer_size, file_offset);
    
    delete buffer;
}


template <typename T>
void remove_deleted_values(std::vector<data<T>> * all_data_vec){
    std::vector<data<T>> after_removal;
    int size_of_all_data = all_data_vec->size();

    for (data<T> &i : (*all_data_vec))
    {
        if (i.is_deleted)
        {
            continue;
        }
        after_removal.push_back(i);
    }
    all_data_vec->swap(after_removal);
    
}


template <typename T>
data<T> * get_block_data(std::ifstream &file, const int block_no , const block_meta<T> &blk_meta){

    if (block_no <= 0) {
        throw std::invalid_argument("block_no must be greater than 0");
    }  

    int column_meta_size = sizeof(column_meta); 
    int block_meta_size = sizeof(block_meta<T>);
    int data_size = sizeof(data<T>);
    int block_size =   block_meta_size + (data_size * RECORDS_LIMIT);

    int offset =  column_meta_size + ((block_no-1) * block_size);

    offset += block_meta_size;  // for current block meta

    data<T> * all_data_frm_blk = new data<T>[blk_meta.count];

    readBinaryFile((char *) all_data_frm_blk,  (blk_meta.count * sizeof(data<T> )),offset,file);

    return all_data_frm_blk;

} 

template <typename T>
data<T> * get_block_data(const std::string &table_name, const std::string &col_name, const int block_no , const block_meta<T> &blk_meta){

    std::ifstream file(get_file_path(table_name,col_name),std::ios::binary);
     
    data<T> * block_data = get_block_data(file,block_no,blk_meta);

    file.close();


    return block_data;
} 

template <typename T>
std::vector<data<T>> * get_block_data(std::string table_name, std::string col_name, const std::vector<int> &block_nos , const std::pair<block_meta<T> *, int> & blk_meta){
    // fetch non deleted values only

    std::ifstream file(get_file_path(table_name,col_name),std::ios::binary);
     
    std::vector<data<T>> *all_data = new std::vector<data<T>>;

    for (int blk_no : block_nos)
    {

        block_meta<T> & single_blk_meta = blk_meta.first[blk_no-1];

        data<T> * block_data = get_block_data(file,blk_no,single_blk_meta);

        all_data->insert(all_data->end(),block_data,block_data+single_blk_meta.count);

        delete[] block_data;
        
    }
    
    remove_deleted_values(all_data);

    file.close();

    return all_data;
}

// Explicit instantiation (not in a header)
template std::vector<data<char>> *  get_block_data<char>(std::string, std::string, const std::vector<int> & , const std::pair<block_meta<char> *, int> &);
template std::vector<data<int>> *  get_block_data<int>(std::string, std::string, const std::vector<int> & , const std::pair<block_meta<int> *, int> &);
template std::vector<data<float>> *  get_block_data<float>(std::string, std::string, const std::vector<int> & , const std::pair<block_meta<float> *, int> &);
template std::vector<data<double>> *  get_block_data<double>(std::string, std::string, const std::vector<int> & , const std::pair<block_meta<double> *, int> &);
template std::vector<data<long>> *  get_block_data<long>(std::string, std::string, const std::vector<int> & , const std::pair<block_meta<long> *, int> &);
template std::vector<data<Dbstr>> *  get_block_data<Dbstr>(std::string, std::string, const std::vector<int> & , const std::pair<block_meta<Dbstr> *, int> &);


template <typename T>
std::vector<data<T>> * get_data_with_rowid(const std::string &table_name, const std::string &col_name, RowID_vector filterd_rows){
    
    std::pair<block_meta<T> *,int> all_block_meta = get_all_block_meta<T>(table_name,col_name);

    std::ifstream col_file(get_file_path(table_name,col_name),std::ios::binary);

    std::vector<data<T>> * all_data = new std::vector<data<T>>;
    
    std::vector<data<T>> &ad = *all_data;
    for (size_t i = 0; i < all_block_meta.second; i++)
    {
        data<T> * blk_data = get_block_data(col_file, (i+1) , all_block_meta.first[i]);

        data<T> * data_count = blk_data+(all_block_meta.first[i].count); // moving ptr to the end
        all_data->insert(all_data->end(),blk_data,data_count);
        delete[] blk_data;
    }

    if(filterd_rows != nullptr){

        if (all_data->size() < filterd_rows->size()) {
            std::string err = "The number of filtered rows exceeds the total number of rows in the column";
            LOG_ERROR(err+ " all-data_size = " + std::to_string(all_data->size())
                + "  filtered-rows-size = " + std::to_string(filterd_rows->size()));
            throw std::runtime_error("The number of filtered rows exceeds the total number of rows in the column.");
        }

        std::vector<data<T>> temp_vec{};
        for (int &row_id : *filterd_rows)
        {
            temp_vec.push_back(all_data->at(row_id));
        }
             

        all_data->swap(temp_vec);
    }
    remove_deleted_values(all_data);
    
    col_file.close();  

    return all_data;    
}

template std::vector<data<char>> * get_data_with_rowid(const std::string &table_name, const std::string &col_name, RowID_vector filterd_rows);
template std::vector<data<int>> * get_data_with_rowid(const std::string &table_name, const std::string &col_name, RowID_vector filterd_rows);
template std::vector<data<float>> * get_data_with_rowid(const std::string &table_name, const std::string &col_name, RowID_vector filterd_rows);
template std::vector<data<double>> * get_data_with_rowid(const std::string &table_name, const std::string &col_name, RowID_vector filterd_rows);
template std::vector<data<long>> * get_data_with_rowid(const std::string &table_name, const std::string &col_name, RowID_vector filterd_rows);
template std::vector<data<Dbstr>> * get_data_with_rowid(const std::string &table_name, const std::string &col_name, RowID_vector filterd_rows);

template<typename T>
std::vector<block_obj<T>> * get_blocks_chunk(std::string table_name, std::string col_name, int block_no, const column_meta &col_meta){
    std::string file_path = get_file_path(table_name, col_name);
    
    std::pair<block_meta<T> *, int> all_blk_meta = get_all_block_meta<T>(table_name, col_name);
    
    int last_block_no = ((col_meta.no_block - block_no) >= BLOCK_LIMIT)?(block_no+BLOCK_LIMIT-1):col_meta.no_block;  // to get 100 blocks from the given block id

    std::ifstream file(file_path, std::ios::binary);

    std::vector<block_obj<T>> *blocks_with_data = new std::vector<block_obj<T>>;
    for (size_t i = block_no; i <= last_block_no; i++)
    {
        block_obj<T> blk;
        blk.meta = all_blk_meta.first[i-1];
        data<T> * blk_data = get_block_data(file,i,blk.meta);
        blk.all_data.assign(blk_data,blk_data+blk.meta.count);
        blocks_with_data->push_back(blk);
        delete blk_data;
    }

    file.close();
    delete all_blk_meta.first;
    
    return blocks_with_data;
}

template std::vector<block_obj<char>> * get_blocks_chunk(std::string, std::string, int, const column_meta &);
template std::vector<block_obj<int>> * get_blocks_chunk(std::string, std::string, int, const column_meta &);
template std::vector<block_obj<float>> * get_blocks_chunk(std::string, std::string, int, const column_meta &);
template std::vector<block_obj<double>> * get_blocks_chunk(std::string, std::string, int, const column_meta &);
template std::vector<block_obj<long>> * get_blocks_chunk(std::string, std::string, int, const column_meta &);
template std::vector<block_obj<Dbstr>> * get_blocks_chunk(std::string, std::string, int, const column_meta &);


template<typename T>
void dump_blocks_chunk(std::string table_name, std::string col_name, std::vector<block_obj<T>> & data_chunk, int block_no){
    if (typeid(T) != typeid(Dbstr))
    {
        block_meta_recalculation<T>(&data_chunk);
    }
    
    int file_offset = sizeof(column_meta) + ((block_no-1) * sizeof(block_meta<T>));
    file_offset +=  (block_no-1) * sizeof(data<T>);
    dump_new_records(data_chunk, table_name,col_name,file_offset);
}

template void dump_blocks_chunk(std::string table_name, std::string col_name, std::vector<block_obj<char>> & data_chunk, int block_no);
template void dump_blocks_chunk(std::string table_name, std::string col_name, std::vector<block_obj<int>> & data_chunk, int block_no);
template void dump_blocks_chunk(std::string table_name, std::string col_name, std::vector<block_obj<float>> & data_chunk, int block_no);
template void dump_blocks_chunk(std::string table_name, std::string col_name, std::vector<block_obj<double>> & data_chunk, int block_no);
template void dump_blocks_chunk(std::string table_name, std::string col_name, std::vector<block_obj<long>> & data_chunk, int block_no);
template void dump_blocks_chunk(std::string table_name, std::string col_name, std::vector<block_obj<Dbstr>> & data_chunk, int block_no);


#endif

