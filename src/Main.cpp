#include "../include/QueryNodes.h"

int main(int argc, char const *argv[])
{
    std::string tableName = "gdp";
    int numColumns;
    std::vector<std::string> columnNames = {"year", "rank", "country","state", "gdp", "gdp_percent"};
    std::vector<int> columnDataTypes = {DBINT,DBINT,DBSTRING,DBSTRING,DBLONG,DBDOUBLE};

    get_home_folder();

    // create_table(tableName,columnNames,columnDataTypes);
    // insert(tableName,"/home/ajith-zstk355/GDP.csv");

    // year,rank,country,state,gdp,gdp_percent

    QueryNode qn;
    qn.selectNode.columns = columnNames;
    qn.selectNode.tableName = tableName;

    FilterNode rn("year", 2020, EQUALS);

    FilterNode ln ("rank",100,EQUALS);


    FilterNode fn(&rn,&ln,AND);

    qn.filterNode = &ln;

    SortNode sn(columnNames.at(1), ASC);

    // qn.isDelete = true;

    // qn.value_to_update.emplace<int>(-1);
    // execute_update(qn);

    execute_select(qn);
    execute_delete(qn);
    qn.filterNode =  nullptr;
    execute_select(qn);


    return 0;
}
