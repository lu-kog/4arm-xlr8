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

    // Constructor for simple conditions
    FilterNode(const std::string& col, const std::string& val, ConditionType cond)
        : columnName(col), value(val), conditionType(cond), left(nullptr), right(nullptr) {}

    // Constructor for compound conditions
    FilterNode(std::unique_ptr<FilterNode> lhs, std::unique_ptr<FilterNode> rhs, ConditionType cond)
        : left(std::move(lhs)), right(std::move(rhs)), conditionType(cond) {}

    void print(){
        if (left) left->print();
        std::cout << "---\nCol Name: " << columnName << "\n" << "Value: " << value << "\n" << "Condition: " << conditionType << "\n---\n";
        if (right) right->print();
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

    void print(){
        std::cout << "Sort Column:" << columnName << std::endl;
        std::cout << "Order: " << sortOrder << std::endl;
    }
};


struct LimitNode
{
    int limit;
    LimitNode(int limit_){
        limit = limit_;
    }

    LimitNode() : limit(-1){};

    void print(){
        std::cout << "Limits: " << limit << std::endl;
    }
};


struct QueryNode {
    SelectNode selectNode;
    std::unique_ptr<FilterNode> filterNode;
    std::unique_ptr<SortNode> sortNode;
    std::unique_ptr<LimitNode> limitNode;
};