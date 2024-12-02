#include "../include/QueryNodes.h"

int main(int argc, char const *argv[])
{
    std::string tableName = "kcc";
    int numColumns;
    std::vector<std::string> columnNames = {"Komi", "Najimi", "Tadano"};
    std::vector<int> columnDataTypes = {DBINT,DBSTRING,DBFLOAT};

    get_home_folder();

    // create_table(tableName,columnNames,columnDataTypes);
    // insert("kcc","/home/gokul-zstk330/temp.csv");

    

    QueryNode qn;
    qn.selectNode.columns = columnNames;
    qn.selectNode.tableName = tableName;

    FilterNode rn("Komi", 1, EQUALS);

    qn.filterNode = &rn;

    SortNode sn(columnNames.at(0), ASC);

    qn.isDelete = true;

    // qn.value_to_update.emplace<int>(-1);
    // execute_update(qn);
    execute_select(qn);
    // execute_delete(qn);
    // execute_select(qn);


    return 0;
}
