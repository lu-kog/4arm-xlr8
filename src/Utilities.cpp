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

    if (!std::filesystem::exists(path)){
        if (std::filesystem::create_directory(path)) {
            std::cout << "Folder created successfully at: " << path << std::endl;
        }
    }

    path += "/";
}

bool create_folder(std::string name){
    std::filesystem::path dir(path+name);
    if (std::filesystem::create_directory(dir)) {
        return true ;
    }
    else 
        return false;
}

bool delete_folder(const std::string& folder_path) {
    try {
        // Check if the folder exists
        if (!fs::exists(folder_path)) {
            std::cerr << "Folder does not exist: " << folder_path << std::endl;
            return false;
        }

        // Remove the folder and its contents
        fs::remove_all(folder_path);

        return true;
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error deleting folder: " << e.what() << std::endl;
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
        std::cerr << "Error reading from file: " << e.what() << std::endl;
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
        std::cerr << "Error reading from file: " << e.what() << std::endl;
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
        std::cerr << "Error writing to a file: " << e.what() << '\n';
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
            std::cout << "Rollback completed for table: " << table_name << ". Data restored from backup." << std::endl;
        } else {
            std::cerr << "Error: No backup found for table " << table_name << "." << std::endl;
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error during rollback: " << e.what() << std::endl;
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
            std::cerr << "Error: Table " << table_name << " does not exist." << std::endl;
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error during backup: " << e.what() << std::endl;
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

// Comparators

