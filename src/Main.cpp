#include "../include/QueryNodes.h"

int main(int argc, char const *argv[])
{
    std::string tableName = "sales_data";
    int numColumns;
    std::vector<std::string> columnNames = {"Region", "Country", "Item_Type", "Order_Priority", "Order_ID", "Units_Sold", "Unit_Price", "Unit_Cost", "Total_Revenue", "Total_Cost", "Total_Profit"};
    std::vector<int> columnDataTypes = {DBSTRING, DBSTRING, DBSTRING, DBCHAR, DBLONG, DBINT, DBDOUBLE, DBDOUBLE, DBDOUBLE, DBDOUBLE, DBDOUBLE};

    // std::vector<std::string> columnNames = {"Country", "Item_Type", "Order_Priority", "Units_Sold", "Total_Revenue"};
    // std::vector<int> columnDataTypes = {DBSTRING, DBSTRING, DBCHAR, DBINT, DBDOUBLE};


    get_home_folder();

    // create_table(tableName,columnNames,columnDataTypes);

    // insert(tableName,"/home/gokul-zstk330/Downloads/SalesRecords.csv");

    QueryNode qn;
    qn.selectNode.columns = columnNames;
    qn.selectNode.tableName = tableName;

    FilterNode rn("year", 2020, EQUALS);

    FilterNode ln ("rank",14,LESS_THAN);


    FilterNode fn(&rn,&ln,AND);

    // qn.filterNode = &ln;

    LimitNode lin(15000);
    qn.limitNode = &lin;
    // SortNode sn(columnNames.at(1), ASC);

    // qn.isDelete = true;

    // qn.value_to_update.emplace<int>(-1);
    // execute_update(qn);
    int a = 10737418240;
    // execute_select(qn);
    // execute_delete(qn);
    // qn.filterNode =  nullptr;
    // execute_select(qn);


    std::cout << a << std::endl;
    return 0;
}
