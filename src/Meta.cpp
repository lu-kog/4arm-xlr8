#include "../include/Meta.h"
#include "../include/Column.h"

template <typename T>
std::pair<block_meta<T> *, int> get_all_block_meta(const std::string &table_name, const std::string &col_name){
    /*
     * Fetch all blocks meta data for given column
    */

    column_meta *curr_col_meta = get_column_meta(table_name, col_name);

    int no_blocks = curr_col_meta->no_block;
    int column_meta_size = sizeof(column_meta);
    int block_size = sizeof(block_meta<T>) + (100 * sizeof(data<T>));

    int offset_for_file = column_meta_size;
    
    block_meta<T> *all_blk_meta  = new block_meta<T>[no_blocks];

    std::ifstream column_file(get_file_path(table_name,col_name),std::ios::binary);
    
    for (size_t i = 0; i < no_blocks; i++)
    {
        readBinaryFile((char *) (all_blk_meta + i), sizeof(block_meta<T>) ,offset_for_file,column_file);

        offset_for_file +=  block_size; 
    }


    delete curr_col_meta;
    return {all_blk_meta,no_blocks};
    

}

// Explicit instantiation (not in a header)
template std::pair<block_meta<char> *, int> get_all_block_meta<char>(const std::string &, const std::string &);
template std::pair<block_meta<int> *, int> get_all_block_meta<int>(const std::string &, const std::string &);
template std::pair<block_meta<float> *, int> get_all_block_meta<float>(const std::string &, const std::string &);
template std::pair<block_meta<double> *, int> get_all_block_meta<double>(const std::string &, const std::string &);
template std::pair<block_meta<long> *, int> get_all_block_meta<long>(const std::string &, const std::string &);
template std::pair<block_meta<Dbstr> *, int> get_all_block_meta<Dbstr>(const std::string &, const std::string &);


template<typename T>
void recalculate_meta(block_meta<T>& meta, const std::vector<data<T>>& block_data) {
    for (const data<T> &d : block_data)
    {
        if (!d.is_deleted)
        {
            if (d.datum > meta.max)
                meta.max = d.datum;
            if (d.datum < meta.min)
                meta.min = d.datum;
        }
    }
}

template<typename T>
void block_meta_recalculation(std::vector<block_obj<T>> * data_chunk){
    for (block_obj<T> &block : *data_chunk)
    {
        recalculate_meta(block.meta, block.all_data);
    }
    
}

template void block_meta_recalculation(std::vector<block_obj<char>> *);
template void block_meta_recalculation(std::vector<block_obj<int>> *);
template void block_meta_recalculation(std::vector<block_obj<float>> *);
template void block_meta_recalculation(std::vector<block_obj<double>> *);
template void block_meta_recalculation(std::vector<block_obj<long>> *);
template void block_meta_recalculation(std::vector<block_obj<Dbstr>> *);
