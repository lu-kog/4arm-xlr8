#include<vector>
#include<string>

struct SelectNode {
    std::vector<std::string> columns;
    std::string tableName;

    SelectNode(const std::string &table, const std::vector<std::string> &cols)
        : tableName(table), columns(cols) {}
};

enum ConditionType { EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, OR, AND };
std::string condition_token[6] = {"=", "!=", "<", ">", "or", "and"};

struct FilterNode {
    std::string columnName;
    std::string value;
    ConditionType conditionType;

    FilterNode *left;
    FilterNode *right; 

    ConditionNode(const std::string &col, const std::string &val, ConditionType cond){
        columnName = col;
        value = val;
        conditionType = cond;
        left = nullptr;
        right = nullptr;
    }
    
    // For conditions with OR, AND
    ConditionNode(ConditionNode *lhs, ConditionNode *rhs, ConditionType cond){
        this.left = lhs;
        this.right = rhs;
        conditionType = cond;
    }
};


enum SortOrder { ASC, DESC };

struct SortNode {
    std::string columnName;
    SortOrder sortOrder;

    SortNode(const std::string &col, SortOrder order = ASC){
        columnName = col;
        sortOrder = order;
    }

};


struct LimitNode
{
    int limit;
    LimitNode(int limit_){
        limit = limit_;
    }
};
