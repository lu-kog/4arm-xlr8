#include <sstream>
#include <iostream>
#include "Node.h"
#include <memory>
#include<stack>
#include<algorithm>

template <typename T>
void print(std::vector<T> input){
    for (T i : input)
    {
        std::cout << i << std::endl;
    }
    
}


void parseSelect(const std::vector<std::string>& tokens, SelectNode& selectNode) {
    std::string from = "from";
    auto it = std::find(tokens.begin(), tokens.end(), from);
    selectNode.columns = std::vector<std::string>(tokens.begin() + 1, it); // Columns between SELECT and FROM
    selectNode.tableName = *(it + 1); // Table name after FROM
}


void processOp(std::stack<std::unique_ptr<FilterNode>>& nodeStack, std::stack<ConditionType>& opStack) {
    auto right = std::move(nodeStack.top());
    nodeStack.pop();
    auto left = std::move(nodeStack.top());
    nodeStack.pop();

    ConditionType op = opStack.top();
    opStack.pop();

    nodeStack.push(std::make_unique<FilterNode>(std::move(left), std::move(right), op));
}

std::unique_ptr<FilterNode> parseConditions(const std::vector<std::string>& tokens, std::vector<std::string>::iterator start) {
    std::stack<std::unique_ptr<FilterNode>> nodeStack;
    std::stack<ConditionType> opStack;

    auto it = start;
    while (it != tokens.end() && *it != "order" && *it != "limit" && *it != ";") {
        if (*it == "or" || *it == "and") {
            ConditionType op = (*it == "or") ? OR : AND;
            while (!opStack.empty() && opStack.top() == AND) {
                processOp(nodeStack, opStack); // Process AND operations with higher precedence
            }
            opStack.push(op);
            ++it;
        } else {
            // Parse a simple condition (e.g., "a > 1")
            std::string colName = *it;
            ConditionType conditionType = EQUALS;
            std::string value;

            if (*(it + 1) == "=") conditionType = EQUALS;
            else if (*(it + 1) == "!=") conditionType = NOT_EQUALS;
            else if (*(it + 1) == "<") conditionType = LESS_THAN;
            else if (*(it + 1) == ">") conditionType = GREATER_THAN;

            value = *(it + 2);
            it += 3; // Move iterator to the next token after the condition

            nodeStack.push(std::make_unique<FilterNode>(colName, value, conditionType));
        }
    }

    while (!opStack.empty()) {
        processOp(nodeStack, opStack);
    }

    return !nodeStack.empty() ? std::move(nodeStack.top()) : nullptr;
}


void parseWhere(const std::vector<std::string>& tokens, std::unique_ptr<FilterNode>& filterNode, std::vector<std::string>::iterator start) {
    filterNode = parseConditions(tokens, start);
}

void parseOrderBy(const std::vector<std::string>& tokens, std::unique_ptr<SortNode>& sortNode, std::vector<std::string>::iterator start) {
    sortNode = std::make_unique<SortNode>(*start); 
    if ((start+1) != tokens.end() && *(start+1) == "desc") {
        sortNode->sortOrder = DESC;
    }
}

void parseLimit(const std::vector<std::string>& tokens, std::unique_ptr<LimitNode>& limitNode, std::vector<std::string>::iterator start) {
    limitNode = std::make_unique<LimitNode>(std::stoi(*start));
}

std::vector<std::string> tokenize(const std::string& qry) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream stream(qry);
    
    while (stream >> token) {
        std::string tempToken;
        for (size_t i = 0; i < token.size(); ++i) {
            if (std::isspace(token[i]) || token[i] == ',') continue; // Skip spaces within tokens

            // Handle multi-character operators
            if (i + 1 < token.size() && (token.substr(i, 2) == "!=")) {
                if (!tempToken.empty()) tokens.push_back(tempToken);
                tokens.push_back("!=");
                i += 1; // Skip next character
                tempToken.clear();
            }
            else if (i + 1 < token.size() && (token.substr(i, 2) == ">=" || token.substr(i, 2) == "<=")) {
                if (!tempToken.empty()) tokens.push_back(tempToken);
                tokens.push_back(token.substr(i, 2));
                i += 1; // Skip next character
                tempToken.clear();
            }
            else if (token[i] == '=' || token[i] == '<' || token[i] == '>') {
                if (!tempToken.empty()) {
                    tokens.push_back(tempToken);
                }
                tokens.push_back(std::string(1, token[i]));
                tempToken.clear();
            }
            else if (token[i] == '(' || token[i] == ')' || token[i] == ';') {
                if (!tempToken.empty()) tokens.push_back(tempToken);
                tokens.push_back(std::string(1, token[i]));
                tempToken.clear();
            }
            else {
                tempToken += token[i];
            }
        }
        if (!tempToken.empty()) {
            tokens.push_back(tempToken);
        }
    }

    return tokens;
}

QueryNode parseQuery(const std::string& query) {
    QueryNode queryNode;

    // Step 1: Tokenize query by spaces
    std::vector<std::string> tokens = tokenize(query); 

    print(tokens);

    // Step 2: Parse SELECT
    parseSelect(tokens, queryNode.selectNode);

    std::cout << "Selected Columns" << std::endl;
    print(queryNode.selectNode.columns);

    // Step 3: Parse WHERE
    auto whereIt = std::find(tokens.begin(), tokens.end(), "where");
    if (whereIt != tokens.end()) {
        parseWhere(tokens, queryNode.filterNode, whereIt + 1);  // Pass iterator to start after "where"
    }

    std::cout << "\n\nConditions: " << std::endl;
    queryNode.filterNode->print(); // checking the tree
    
    // Step 4: Parse ORDER BY
    auto orderByIt = std::find(tokens.begin(), tokens.end(), "order");
    if (orderByIt+1 != tokens.end() && *(orderByIt + 1) == "by") {
        parseOrderBy(tokens, queryNode.sortNode, orderByIt + 2);  // Start after "order by"
        queryNode.sortNode->print();
    }else{
        queryNode.sortNode = nullptr;
    }

    // Step 5: Parse LIMIT
    auto limitIt = std::find(tokens.begin(), tokens.end(), "limit");
    if (limitIt != tokens.end()) {
        parseLimit(tokens, queryNode.limitNode, limitIt + 1);  // Start after "limit"
        queryNode.limitNode->print();
    }else{
        queryNode.limitNode = nullptr;
    }
    
    return queryNode;
}



int main() {
    std::string query = "select col1, col2 from table1 where col1 != 5 and col2 > 10;";
    std::string query2 = "SELECT col1, col2 FROM table1;";
    std::string query3 = "SELECT * FROM table1;";
    std::string query4 = "SELECT col1, col2 FROM table1 WHERE col1 = 'value1';";
    std::string query5 = "SELECT col1, col2 FROM table1 WHERE col1 > 10 AND col2 < 20;";
    std::string query6 = "SELECT col1, col2 FROM table1 WHERE (col1 > 5 AND col2 < 15) OR col3 = 'C';";
    std::string query7 = "select col1, col2 from table1 where col1 > 10 order by col2 desc limit 3;";

    parseQuery(query7);
    
    return 0;
}
