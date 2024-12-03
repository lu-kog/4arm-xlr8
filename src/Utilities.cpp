#include "../include/Utilities.h"


/*--------------------------------------------------------------------*/


// File IO functions

std::string get_file_path (const std::string& table_name,  const std::string &col_name){
    std::string path = get_path() + table_name + "/"+col_name;
    return path;
}

std::string get_path(){
    return path;
}

void get_home_folder(){
    path = std::getenv("HOME");

    path += "/oursql";

    if (!fs::exists(path)){
        if (fs::create_directory(path)) {
            std::cout << "Folder created successfully at: " << path << std::endl;
        }else{
            throw std::runtime_error("Can't create data directory!");
        }
        
    }

    path += "/";
    Logger::getInstance().setLogLevel(Logger::LogLevel::INFO);
    Logger::getInstance().setLogFile(path+".log");

}

bool create_folder(std::string name){
    fs::path dir(path+name);
    if (fs::create_directory(dir)) {
        return true ;
    }
    else 
        return false;
}

bool delete_folder(const std::string& folder_path) {
    try {
        // Check if the folder exists
        if (!fs::exists(folder_path)) {
            LOG_ERROR("Delete Folder: Folder does not exist - " + folder_path);
            return false;
        }

        // Remove the folder and its contents
        fs::remove_all(folder_path);

        return true;
    } catch (const fs::filesystem_error& e) {
        std::string err_name = e.what();
        LOG_ERROR("Error deleting folder - " + folder_path + err_name);
        throw std::runtime_error("Error deleting folder");
    }
    return false;
}

void readBinaryFile(const std::string &filename, char *buffer, long long size, int offset)
{
    try
    {
        std::ifstream inFile(filename, std::ios::binary);

        // Enable exceptions for failure
        inFile.exceptions(std::ifstream::failbit | std::ifstream::badbit);

        // Move the file pointer to the specified offset
        inFile.seekg(offset, std::ios::beg);

        // Read 'size' bytes into the buffer
        inFile.read(buffer, size);

        inFile.close();
    }
    catch (const std::ios_base::failure &e)
    {
        std::string err_name(e.what());
        LOG_ERROR("Error reading binary - " + filename + err_name);
        throw std::runtime_error( "Error reading from file: "+err_name);
    }
}

void readBinaryFile(char *buffer, long long size, int offset, std::ifstream &inFile)
{
    try
    {
        // Enable exceptions for failure
        inFile.exceptions(std::ifstream::failbit | std::ifstream::badbit);

        // Move the file pointer to the specified offset
        inFile.seekg(offset, std::ios::beg);

        // Read 'size' bytes into the buffer
        inFile.read(buffer, size);

    }
    catch (const std::ios_base::failure &e)
    {
        std::string err_name(e.what());
        LOG_ERROR("Error reading binary - " + err_name);
        throw std::runtime_error( "Error reading from file: "+err_name);

    }
}

void writeBinaryFile(const std::string &filename, const char *buffer, long long size, long long offSet)
{
    try
    {
        // std::ofstream outFile(filename, std::ios::binary | std::ios::in | std::ios::out);


        std::ofstream outFile;
        
        // Check if the file exists using std::filesystem
        if (std::filesystem::exists(filename))
        {
            // If the file exists, open it with both std::ios::in and std::ios::out (read and write)
            outFile.open(filename, std::ios::binary | std::ios::in | std::ios::out);
        }
        else
        {
            // If the file doesn't exist, open it with std::ios::out to create it
            outFile.open(filename, std::ios::binary | std::ios::out);
        }
        

        outFile.exceptions(std::ofstream::failbit | std::ofstream::badbit);
        outFile.seekp(offSet, std::ios::beg);

        
        outFile.write(buffer, size);
        outFile.close();
    }
    catch (const std::ios_base::failure &e)
    {
        std::string err_name(e.what());
        LOG_ERROR("Error writing to a file - " + filename + err_name);
        throw std::runtime_error( "Error writing to a file: "+err_name);
    }
}

int get_size(std::string file_name){
    std::ifstream in_file(file_name, std::ios::binary);
   in_file.seekg(0, std::ios::end);
   int file_size = in_file.tellg();
   return file_size;
}

void Roll_Back(const std::string& table_name) {
    std::string table_path = get_path() + table_name; 
    std::string backup_path = get_path() + ".backup/" + table_name;

    try {
        // Check if backup exists
        if (fs::exists(backup_path)) {
            // Remove current table data
            if (fs::exists(table_path)) {
                fs::remove_all(table_path);
            }

            // Restore from backup
            fs::copy(backup_path, table_path, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
            LOG_INFO("Rollback completed for table - " + table_name);
        } else {
            LOG_ERROR("No backup found for table - " + table_name);
        }
    } catch (const fs::filesystem_error& e) {
        std::string err_name(e.what());
        LOG_ERROR("Error on rollback for table - " + table_name);
    }
}

void backup_table_data(const std::string& table_name) {
    std::string table_path = get_path() + table_name; 
    std::string backup_dir = get_path() + ".backup/";
    std::string backup_path = backup_dir + table_name;

    try {
        // Ensure the backup directory exists
        if (!fs::exists(backup_path)) {
            fs::create_directory(backup_dir);
            fs::create_directory(backup_path);
        }

        // Copy table data to backup directory
        if (fs::exists(table_path)) {
            fs::copy(table_path, backup_path, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
        } else {
            LOG_ERROR("Table Data not found to backup. Table - " + table_name);
        }
    } catch (const fs::filesystem_error& e) {
        LOG_ERROR("Error on backup for table - " + table_name);
    }
}


/*--------------------------------------------------------------------*/

 // QueryNode helper functions

RowID_vector mergeAndRemoveDuplicates(RowID_vector filtered_left_row, RowID_vector filtered_right_row) {
    std::set<int> uniqueElements(filtered_left_row->begin(), filtered_left_row->end());
    uniqueElements.insert(filtered_right_row->begin(), filtered_right_row->end());

    delete filtered_left_row, filtered_right_row; // free memory

    RowID_vector result = new std::vector<int>(uniqueElements.begin(), uniqueElements.end());
    
    return result;
}


/*-----------------------------------------------------------------------*/
