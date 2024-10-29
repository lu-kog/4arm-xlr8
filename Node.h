#include<vector>
#include<string>
#include <memory>

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

    std::unique_ptr<FilterNode> left;
    std::unique_ptr<FilterNode> right;

    // Constructor for leaf nodes
    FilterNode(const std::string &col, const std::string &val, ConditionType cond)
        : columnName(col), value(val), conditionType(cond), left(nullptr), right(nullptr) {}

    // Constructor for OR, AND conditions with unique_ptrs
    FilterNode(std::unique_ptr<FilterNode> lhs, std::unique_ptr<FilterNode> rhs, ConditionType cond)
        : conditionType(cond), left(std::move(lhs)), right(std::move(rhs)) {}

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
