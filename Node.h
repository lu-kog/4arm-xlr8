#include<vector>
#include<string>
#include <memory>
#include <meta.h>
#include <set>
#include<unordered_set>
#include<variant>

typedef std::vector<int>* RowID_vector;

struct SelectNode {
    std::vector<std::string> columns;
    std::string tableName;
};

enum ConditionType { EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, OR, AND };
std::string condition_token[6] = {"=", "!=", "<", ">", "or", "and"};

struct FilterNode {
    std::string columnName; // must be int column
    union{
        char c; int i; long l; float f; double d;
    } value;  
    int data_type;
    ConditionType conditionType;
    std::unique_ptr<FilterNode> left;
    std::unique_ptr<FilterNode> right;

    // Overloaded Constructors for simple conditions
    FilterNode(const std::string& col, char val, ConditionType cond)
        : columnName(col), data_type(DBCHAR), conditionType(cond), left(nullptr), right(nullptr) { value.c = val; }
    FilterNode(const std::string& col, int val, ConditionType cond)
        : columnName(col), data_type(DBINT), conditionType(cond), left(nullptr), right(nullptr) { value.i = val; }
    FilterNode(const std::string& col, long val, ConditionType cond)
        : columnName(col), data_type(DBLONG), conditionType(cond), left(nullptr), right(nullptr) { value.l = val; }
    FilterNode(const std::string& col, float val, ConditionType cond)
        : columnName(col), data_type(DBFLOAT), conditionType(cond), left(nullptr), right(nullptr) { value.f = val; }
    FilterNode(const std::string& col, double val, ConditionType cond)
        : columnName(col), data_type(DBDOUBLE), conditionType(cond), left(nullptr), right(nullptr) { value.d = val; }


    // Constructor for compound conditions
    FilterNode(std::unique_ptr<FilterNode> lhs, std::unique_ptr<FilterNode> rhs, ConditionType cond)
        : left(std::move(lhs)), right(std::move(rhs)), conditionType(cond) {}

    void print(){
        if (left) left->print();
        std::cout << "---\nCol Name: " << columnName << "\n" << "Value: " << value << "\n" << "Condition: " << conditionType << "\n---\n";
        if (right) right->print();
    }


    RowID_vector mergeAndRemoveDuplicates(RowID_vector vec1, RowID_vector vec2);
    RowID_vector execute(const std::string& table_name);
    RowID_vector execute(const std::string& table_name, RowID_vector row_ids);

    template <typename T>
    RowID_vector apply_filter(const std::string& table_name, RowID_vector rows_to_process = nullptr);

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