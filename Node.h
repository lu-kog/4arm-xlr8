#include<vector>
#include<string>
#include <memory>

struct SelectNode {
    std::vector<std::string> columns;
    std::string tableName;
};

enum ConditionType { EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, OR, AND };
std::string condition_token[6] = {"=", "!=", "<", ">", "or", "and"};

struct FilterNode {
    std::string columnName;
    std::string value;
    ConditionType conditionType;
    std::unique_ptr<FilterNode> left;
    std::unique_ptr<FilterNode> right;

    FilterNode(const std::string& col, const std::string& val, ConditionType cond)
        : columnName(col), value(val), conditionType(cond), left(nullptr), right(nullptr) {}
};


enum SortOrder { ASC=1, DESC };

struct SortNode {
    std::string columnName;
    SortOrder sortOrder;

    SortNode(const std::string &col, SortOrder order = ASC){
        columnName = col;
        sortOrder = order;
    }

    SortNode(){
        sortOrder = 0; // default asc?? // On which column??
    }

};


struct LimitNode
{
    int limit;
    LimitNode(int limit_){
        limit = limit_;
    }

    LimitNode() : limit(-1){};
};


struct QueryNode {
    SelectNode selectNode;
    std::unique_ptr<FilterNode> filterNode;
    std::unique_ptr<SortNode> sortNode;
    std::unique_ptr<LimitNode> limitNode;
};