#pragma once

#ifndef _TEST_CASES_SELECT
#define _TEST_CASES_SELECT 0

#include "Node.h"

QueryNode qn1;
QueryNode qn2;
QueryNode qn3;
QueryNode qn4;
QueryNode qn5;
QueryNode qn6;
QueryNode qn7;
QueryNode qn8;
QueryNode qn9;
QueryNode qn10;

void init_test()
{
    /* Test Case 1: Basic Select Without Filters */
    qn1.selectNode.columns = {"Komi", "Tadano"};
    qn1.selectNode.tableName = "kcc";

    /* Test Case 2: Select With a Single Filter */

    qn2.selectNode.columns = {"Komi"};
    qn2.selectNode.tableName = "kcc";

    FilterNode* filter2 = new FilterNode("Komi", 20, GREATER_THAN);
    qn2.filterNode = filter2;

    /* Test Case 3: Select With Multiple Filters (AND) */

    qn3.selectNode.columns = {"Najimi", "Tadano"};
    qn3.selectNode.tableName = "kcc";

    FilterNode left3("Najimi", 50.0f, LESS_THAN);
    FilterNode right3("Tadano", 30, EQUALS);
    FilterNode root3(&left3, &right3, AND);

    qn3.filterNode = &root3;

    /* Test Case 4: Select With Multiple Filters (OR) */

    qn4.selectNode.columns = {"Tadano"};
    qn4.selectNode.tableName = "kcc";

    FilterNode left4("Najimi", 100, GREATER_THAN);
    FilterNode right4("Tadano", 5.5f, LESS_THAN);
    FilterNode root4(&left4, &right4, OR);

    qn4.filterNode = &root4;

    /* Test Case 5: Select With Sort */

    qn5.selectNode.columns = {"Najimi"};
    qn5.selectNode.tableName = "kcc";

    SortNode sort5("Najimi", ASC);
    qn5.sortNode = &sort5;

    /* Test Case 6: Select With Filter and Sort */

    qn6.selectNode.columns = {"Tadano"};
    qn6.selectNode.tableName = "kcc";

    FilterNode filter6("Najimi", 40.5f, GREATER_THAN);
    qn6.filterNode = &filter6;

    SortNode sort6("Tadano", DESC);
    qn6.sortNode = &sort6;

    /* Test Case 7: Select With Filter, Sort, and Limit */

    qn7.selectNode.columns = {"Najimi"};
    qn7.selectNode.tableName = "kcc";

    FilterNode filter7("Najimi", 10, GREATER_THAN);
    qn7.filterNode = &filter7;

    SortNode sort7("Najimi", ASC);
    qn7.sortNode = &sort7;

    LimitNode limit7(5);
    qn7.limitNode = &limit7;

    /* Test Case 8: Select With Complex Filters (Nested AND and OR) */

    qn8.selectNode.columns = {"Komi", "Tadano"};
    qn8.selectNode.tableName = "kcc";

    FilterNode left8("Najimi", 10, GREATER_THAN);
    FilterNode right8("Tadano", 20.5f, LESS_THAN);
    FilterNode subRoot8(&left8, &right8, AND);

    FilterNode outerRight8("Komi", 50, GREATER_THAN);
    FilterNode root8(&subRoot8, &outerRight8, OR);

    qn8.filterNode = &root8;

    /* Test Case 9: Select All Columns Without Any Filters */

    qn9.selectNode.columns = {"Komi", "Najimi", "Tadano"};
    qn9.selectNode.tableName = "kcc";

    /* Test Case 10: Select With Filters and Limit */

    qn10.selectNode.columns = {"Komi"};
    qn10.selectNode.tableName = "kcc";

    FilterNode filter10("Komi", 30, LESS_THAN);
    qn10.filterNode = &filter10;

    LimitNode limit10(3);
    qn10.limitNode = &limit10;
}

#endif