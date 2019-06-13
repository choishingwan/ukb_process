#include "misc.hpp"
#include <cstdlib>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <sqlite3.h>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <unordered_set>
#include <vector>

static int callback(void* /*NotUsed*/, int argc, char** argv, char** azColName)
{
    int i;
    for (i = 0; i < argc; i++) {
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

void create_tables(sqlite3* db)
{
    int rc;
    char* zErrMsg = nullptr;
    std::string sql;

    sql = "CREATE TABLE CODE("
          "ID INT PRIMARY KEY NOT NULL);";
    rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    else
    {
        fprintf(stdout, "Table:CODE created successfully\n");
    }
    sql = "CREATE TABLE CODE_META("
          "ID INT,"
          "Value INT NOT NULL,"
          "Meaning TEXT,"
          "FOREIGN KEY (ID) REFERENCES CODE(ID));";
    rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    else
    {
        fprintf(stdout, "Table:CODE_META created successfully\n");
    }
    sql = "CREATE TABLE SAMPLE("
          "ID INT PRIMARY KEY NOT NULL,"
          "DropOut BOOLEAN);";
    rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    else
    {
        fprintf(stdout, "Table:SAMPLE created successfully\n");
    }
    sql = "CREATE TABLE PHENO_META("
          "Path TEXT,"
          "Category INT NOT NULL,"
          "FieldID INT PRIMARY KEY NOT NULL,"
          "Field TEXT NOT NULL,"
          "Participants INT NOT NULL,"
          "Items INT NOT NULL,"
          "Stability TEXT NOT NULL,"
          "ValueType TEXT NOT NULL,"
          "Units TEXT, "
          "ItemType TEXT,"
          "Strata TEXT,"
          "Sexed TEXT,"
          "Instances INT NOT NULL,"
          "Array INT NOT NULL,"
          "Coding INT,"
          "Notes TEXT,"
          "FOREIGN KEY (Coding) REFERENCES CODE(ID));";
    rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    else
    {
        fprintf(stdout, "Table:PHENO_META created successfully\n");
    }
}


void load_code(sqlite3* db, const std::string& code_showcase)
{
    std::ifstream code(code_showcase.c_str());
    if (!code.is_open()) {
        std::string error_message =
            "Error: Cannot open code showcase file: " + code_showcase
            + ". Please check you have the correct input";
        throw std::runtime_error(error_message);
    }
    std::string line;
    std::string sql;
    int rc;
    char* zErrMsg = nullptr;
    // there is a header
    std::getline(code, line);
    std::cerr << "Header line of code showcase: " << std::endl;
    std::cerr << line << std::endl;
    std::vector<std::string> token;
    std::unordered_set<std::string> id;
    while (std::getline(code, line)) {
        misc::trim(line);
        if (line.empty()) continue;
        // CSV input
        token = misc::split(line, ",");
        if (token.size() < 3) {
            std::string error_message =
                "Error: Undefined Code Showcase "
                "format! File is expected to have exactly 3 columns.\n"
                + line;
            throw std::runtime_error(error_message);
        }
        for (size_t i = 4; i < token.size(); ++i) {
            token[3].append("," + token[i]);
        }
        if (id.find(token[0]) == id.end()) {
            // ADD this into CODE table
            sql = "INSERT INTO CODE(ID) SELECT " + token[0]
                  + " WHERE NOT EXISTS(SELECT 1 "
                    "FROM CODE WHERE ID=="
                  + token[0] + ");";
            rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
            }
        }
        // NOW ADD OR INSERT TO CODE_META
        sql = "INSERT INTO CODE_META(ID, Value, Meaning) SELECT " + token[0]
              + "," + token[1] + ",\"" + token[2]
              + "\" WHERE NOT EXISTS(SELECT 1 "
                "FROM CODE WHERE ID=="
              + token[0] + " AND Value==" + token[1] + " AND Meaning==\""
              + token[2] + "\");";
    }
    code.close();
}

int main(int argc, char* argv[])
{
    static const char* optString = "d:c:p:o:?";
    static const struct option longOpts[] = {
        {"data", required_argument, nullptr, 'd'},
        {"code", required_argument, nullptr, 'c'},
        {"pheno", required_argument, nullptr, 'p'},
        {"out", required_argument, nullptr, 'o'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0}};
    int longIndex = 0;
    int opt = 0;
    opt = getopt_long(argc, argv, optString, longOpts, &longIndex);
    std::string data_showcase, code_showcase, pheno_name, out_name;
    while (opt != -1) {
        switch (opt)
        {
        case 'd': data_showcase = optarg; break;
        case 'c': code_showcase = optarg; break;
        case 'p': pheno_name = optarg; break;
        case 'o': out_name = optarg; break;
        case 'h':
        case '?': return 0;
        default:
            throw "Undefined operator, please use --help for more "
                  "information!";
        }
        opt = getopt_long(argc, argv, optString, longOpts, &longIndex);
    }
    bool error = false;
    if (data_showcase.empty()) {
        error = true;
        std::cerr << "Error: You must provide the data showcase csv file!"
                  << std::endl;
    }
    if (code_showcase.empty()) {
        error = true;
        std::cerr << "Error: You must provide the code showcase csv file!"
                  << std::endl;
    }
    if (pheno_name.empty()) {
        error = true;
        std::cerr << "Error: You must provide the phenotype!" << std::endl;
    }
    if (out_name.empty()) {
        error = true;
        std::cerr << "Error: You must provide output prefix!" << std::endl;
    }
    if (error) {
        std::cerr << "Please check you have all the required input!"
                  << std::endl;
        return -1;
    }
    std::string db_name = out_name + ".db";
    sqlite3* db;
    bool create_table = true;
    if (misc::file_exists(db_name)) {
        create_table = false;
    }
    int rc = sqlite3_open(db_name.c_str(), &db);
    if (rc) {
        std::cerr << "Cannot open database: " << db_name << std::endl;
        return -1;
    }
    else
    {
        std::cerr << "Opened database: " << db_name << std::endl;
    }
    if (create_table) create_tables(db);
    load_code(db, code_showcase);

    sqlite3_close(db);
    return 0;
}
