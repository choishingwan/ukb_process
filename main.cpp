#include "misc.hpp"
#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <sqlite3.h>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

typedef std::pair<std::string, std::string> pheno_info;
static int callback(void* /*NotUsed*/, int argc, char** argv, char** azColName)
{
    int i;
    for (i = 0; i < argc; i++)
    { printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL"); }
    printf("\n");
    return 0;
}

void create_tables(sqlite3* db, const std::string& memory)
{
    int rc;
    char* zErrMsg = nullptr;
    std::string sql;
    sqlite3_exec(db, std::string("PRAGMA cache_size = " + memory).c_str(),
                 nullptr, nullptr, &zErrMsg);
    sql = "CREATE TABLE CODE("
          "ID INT PRIMARY KEY NOT NULL);";
    rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
    if (rc != SQLITE_OK)
    {
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
    if (rc != SQLITE_OK)
    {
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
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    else
    {
        fprintf(stdout, "Table:SAMPLE created successfully\n");
    }
    sql = "CREATE TABLE DATA_META("
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
          "Included BOOLEAN,"
          "FOREIGN KEY (Coding) REFERENCES CODE(ID));";
    rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    else
    {
        fprintf(stdout, "Table:DATA_META created successfully\n");
    }

    sql = "CREATE TABLE PHENO_META("
          "ID INT PRIMARY KEY NOT NULL,"
          "FieldID INT NOT NULL,"
          "Pheno TEXT NOT NULL,"
          "FOREIGN KEY (FieldID) REFERENCES DATA_META(FieldID));";
    rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    else
    {
        fprintf(stdout, "Table:PHENO_META created successfully\n");
    }

    sql = "CREATE TABLE PHENOTYPE("
          "ID INT NOT NULL,"
          "Instance INT NOT NULL,"
          "PhenoID INT NOT NULL,"
          "FOREIGN KEY (ID) REFERENCES SAMPLE(ID),"
          "FOREIGN KEY (PhenoID) REFERENCES PHENO_META(ID));";
    rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    else
    {
        fprintf(stdout, "Table:PHENOTYPE created successfully\n");
    }
}


void load_code(sqlite3* db, const std::string& code_showcase)
{
    std::ifstream code(code_showcase.c_str());
    if (!code.is_open())
    {
        std::string error_message =
            "Error: Cannot open code showcase file: " + code_showcase
            + ". Please check you have the correct input";
        throw std::runtime_error(error_message);
    }
    std::string line;
    std::string sql;
    char* zErrMsg = nullptr;
    // there is a header
    code.seekg(0, code.end);
    auto file_length = code.tellg();
    code.clear();
    code.seekg(0, code.beg);
    std::getline(code, line);
    std::cerr << std::endl
              << "============================================================"
              << std::endl;
    std::cerr << "Header line of code showcase: " << std::endl;
    std::cerr << line << std::endl;
    double prev_percentage = 0;
    std::vector<std::string> token;
    std::unordered_set<std::string> id;
    sqlite3_stmt* code_stat;
    sqlite3_stmt* code_meta_stat;
    std::string code_statement = "INSERT INTO CODE(ID) VALUES(@ID)";
    std::string code_meta_statement =
        "INSERT INTO CODE_META(ID, Value, Meaning) VALUES(@ID,@V, @M)";
    sqlite3_prepare_v2(db, code_statement.c_str(), -1, &code_stat, nullptr);
    sqlite3_prepare_v2(db, code_meta_statement.c_str(), -1, &code_meta_stat,
                       nullptr);
    sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, &zErrMsg);

    while (std::getline(code, line))
    {
        misc::trim(line);
        if (line.empty()) continue;
        double cur_progress = (static_cast<double>(code.tellg())
                               / static_cast<double>(file_length))
                              * 100.0;
        // progress bar can be slow when permutation + thresholding is used due
        // to the huge amount of processing required
        if (cur_progress - prev_percentage > 0.01)
        {
            fprintf(stderr, "\rProcessing %03.2f%%", cur_progress);
            prev_percentage = cur_progress;
        }

        if (prev_percentage >= 100.0)
        { fprintf(stderr, "\rProcessing %03.2f%%", 100.0); }
        // CSV input
        token = misc::csv_split(line);
        if (token.size() != 3)
        {
            std::string error_message =
                "Error: Undefined Code Showcase "
                "format! File is expected to have exactly 3 columns.\n"
                + line;
            throw std::runtime_error(error_message);
        }
        for (size_t i = 3; i < token.size(); ++i)
        { token[2] = token[2] + "," + token[i]; }
        // clean up the string
        token[2].erase(std::remove(token[2].begin(), token[2].end(), '\"'),
                       token[2].end());
        token[2] = "\"" + token[2] + "\"";
        if (id.find(token[0]) == id.end())
        {
            // ADD this into CODE table
            sqlite3_bind_text(code_stat, 1, token[0].c_str(), -1,
                              SQLITE_TRANSIENT);
            int status = sqlite3_step(code_stat);
            if (status != SQLITE_DONE || status == SQLITE_ERROR
                || status == SQLITE_BUSY)
            {
                std::string errorMessage(sqlite3_errmsg(db));
                throw std::runtime_error("Error: Insert failed: " + errorMessage
                                         + " (" + std::to_string(status) + ")");
            }
            sqlite3_clear_bindings(code_stat);
            sqlite3_reset(code_stat);
            id.insert(token[0]);
        }
        sqlite3_bind_text(code_meta_stat, 1, token[0].c_str(), -1,
                          SQLITE_TRANSIENT);
        sqlite3_bind_text(code_meta_stat, 2, token[1].c_str(), -1,
                          SQLITE_TRANSIENT);
        sqlite3_bind_text(code_meta_stat, 3, token[2].c_str(), -1,
                          SQLITE_TRANSIENT);
        int status = sqlite3_step(code_meta_stat);
        if (status != SQLITE_DONE || status == SQLITE_ERROR
            || status == SQLITE_BUSY)
        {
            std::string errorMessage(sqlite3_errmsg(db));
            throw std::runtime_error("Error: Insert failed: " + errorMessage
                                     + " (" + std::to_string(status) + ")");
        }
        sqlite3_clear_bindings(code_meta_stat);
        sqlite3_reset(code_meta_stat);
    }
    code.close();
    sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
    sqlite3_exec(db, "CREATE INDEX 'CODE_META_Index' ON 'CODE_META' ('ID')",
                 nullptr, nullptr, &zErrMsg);
    fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
}

void load_data(sqlite3* db,
               const std::unordered_set<std::string>& included_fields,
               const std::string& data_showcase)
{
    std::cerr << "Total " << included_fields.size() << " to be included"
              << std::endl;
    std::ifstream data(data_showcase.c_str());
    if (!data.is_open())
    {
        std::string error_message =
            "Error: Cannot open data showcase file: " + data_showcase
            + ". Please check you have the correct input";
        throw std::runtime_error(error_message);
    }
    std::string line;
    std::string sql;
    char* zErrMsg = nullptr;
    // there is a header
    data.seekg(0, data.end);
    auto file_length = data.tellg();
    data.clear();
    data.seekg(0, data.beg);
    std::getline(data, line);
    std::cerr << std::endl
              << "============================================================"
              << std::endl;
    std::cerr << "Header line of data showcase: " << std::endl;
    std::cerr << line << std::endl;
    double prev_percentage = 0;
    std::vector<std::string> token;
    sqlite3_stmt* dat_stat;
    std::string data_statement =
        "INSERT INTO DATA_META(Category, FieldID, Field, Participants, "
        "Items, Stability, ValueType, Units, ItemType, Strata, Sexed, "
        "Instances, Array, Coding, Included) "
        "VALUES(@CATEGORY,@FIELDID,@FIELD,@PARTICIPANTS,@ITEM,@STABILITY,"
        "@VALUETYPE,@UNITS,@ITEMTYPE,@STRATA,@SEXED,@INSTANCES,@ARRAY,@CODING, "
        "@INCLUDED)";
    sqlite3_prepare_v2(db, data_statement.c_str(), -1, &dat_stat, nullptr);
    sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, &zErrMsg);

    while (std::getline(data, line))
    {
        misc::trim(line);
        if (line.empty()) continue;
        double cur_progress = (static_cast<double>(data.tellg())
                               / static_cast<double>(file_length))
                              * 100.0;
        // progress bar can be slow when permutation + thresholding is used due
        // to the huge amount of processing required
        if (cur_progress - prev_percentage > 0.01)
        {
            fprintf(stderr, "\rProcessing %03.2f%%", cur_progress);
            prev_percentage = cur_progress;
        }

        if (prev_percentage >= 100.0)
        { fprintf(stderr, "\rProcessing %03.2f%%", 100.0); }
        // CSV input
        token = misc::csv_split(line);
        if (token.size() != 17)
        {
            std::string error_message =
                "Error: Undefined Data Showcase "
                "format! File is expected to have exactly 17 columns.\n"
                + line;
            throw std::runtime_error(error_message);
        }
        if (included_fields.find(token[3]) == included_fields.end())
        { continue; }
        std::cerr << token[3] << " found" << std::endl;
        for (size_t i = 1; i < 15; ++i)
        {
            // we skip the first one and last 2 as they are not as useful
            // can always retrieve those using data showcase
            if (i == 3 || (i >= 6 && i <= 11))
            {
                token[i].erase(
                    std::remove(token[i].begin(), token[i].end(), '\"'),
                    token[i].end());
                if (token[i] != "NULL") token[i] = "\"" + token[i] + "\"";
            }
            sqlite3_bind_text(dat_stat, static_cast<int>(i), token[i].c_str(),
                              -1, SQLITE_TRANSIENT);
        }
        sqlite3_bind_text(
            dat_stat, 15,
            (included_fields.find(token[3]) == included_fields.end()) ? "0"
                                                                      : "1",
            2, SQLITE_TRANSIENT);
        int status = sqlite3_step(dat_stat);
        if (status != SQLITE_DONE || status == SQLITE_ERROR
            || status == SQLITE_BUSY)
        {
            std::string errorMessage(sqlite3_errmsg(db));
            throw std::runtime_error("Error: Insert failed: " + errorMessage
                                     + " (" + std::to_string(status) + ")");
        }
        sqlite3_clear_bindings(dat_stat);
        sqlite3_reset(dat_stat);
    }
    data.close();
    sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
    fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
    sqlite3_exec(db, "CREATE INDEX 'DATA_Index' ON 'DATA_META' ('FieldID')",
                 nullptr, nullptr, &zErrMsg);
}

std::vector<pheno_info> get_pheno_meta(const std::string& pheno,
                                       std::vector<std::string>& token,
                                       std::unordered_set<std::string>& fields,
                                       size_t& id_idx)
{
    std::vector<std::string> subtoken;
    std::vector<pheno_info> phenotype_meta;
    std::unordered_set<std::string> processed_field;
    std::string field_id, instance_num;
    for (size_t i = 0; i < token.size(); ++i)
    {

        // remove "
        token[i].erase(std::remove(token[i].begin(), token[i].end(), '\"'),
                       token[i].end());
        if (token[i] == "f.eid")
        {
            phenotype_meta.emplace_back(std::make_pair("0", "0"));
            id_idx = i;
        }
        else
        {
            // we don't care about the array index
            subtoken = misc::split(token[i], ".");
            if (subtoken.size() != 4)
            {
                throw std::runtime_error("Error: We expect all Field ID "
                                         "from the phenotype to have the "
                                         "following format: f.x.x.x: "
                                         + token[i]);
            }
            // if(pheno_id.find(subtoken[1])==pheno_id.end()){
            field_id = subtoken[1];
            instance_num = subtoken[2];
            if (processed_field.find(field_id) == processed_field.end()
                && fields.find(field_id) != fields.end())
            {
                // When we read the second phentype file, we found that
                // it was already read, so we should skip it an issue a
                // warning
                fprintf(stderr,
                        "Warning: Duplicated Field ID (%s) detected in %s. "
                        "We will ignore this instance\n",
                        token[i].c_str(), pheno.c_str());
                // use NA to indicate we want this to be ignored
                phenotype_meta.emplace_back(std::make_pair("NA", instance_num));
            }
            else
            {
                fields.insert(field_id);
                phenotype_meta.emplace_back(
                    std::make_pair(field_id, instance_num));
                processed_field.insert(field_id);
            }
        }
    }
    return phenotype_meta;
}

void update_pheno_meta_db(sqlite3* db, sqlite3_stmt* insert_pheno_meta,
                          const std::string& pheno_id,
                          const std::string& field_id, const std::string& pheno)
{
    sqlite3_bind_text(insert_pheno_meta, 1, pheno_id.c_str(), -1,
                      SQLITE_TRANSIENT);
    sqlite3_bind_text(insert_pheno_meta, 2, field_id.c_str(), -1,
                      SQLITE_TRANSIENT);
    sqlite3_bind_text(insert_pheno_meta, 3, pheno.c_str(), -1,
                      SQLITE_TRANSIENT);
    int status = sqlite3_step(insert_pheno_meta);
    if (status != SQLITE_DONE || status == SQLITE_ERROR
        || status == SQLITE_BUSY)
    {
        std::string errorMessage(sqlite3_errmsg(db));
        throw std::runtime_error("Error: Insert failed: " + errorMessage + " ("
                                 + std::to_string(status) + ")");
    }
    sqlite3_clear_bindings(insert_pheno_meta);
    sqlite3_reset(insert_pheno_meta);
}
size_t get_phenotype_id(
    sqlite3* db, sqlite3_stmt* insert_pheno_meta,
    std::unordered_map<std::string, std::unordered_map<std::string, size_t>>&
        pheno_id,
    size_t& pheno_meta_idx, const std::string& field_id,
    const std::string& pheno)
{
    auto&& field_loc = pheno_id.find(field_id);
    if (field_loc != pheno_id.end())
    {
        auto&& pheno_loc = field_loc->second.find(pheno);
        if (pheno_loc != field_loc->second.end()) { return pheno_loc->second; }
        else
        {
            pheno_id[field_id][pheno] = pheno_meta_idx;
            update_pheno_meta_db(db, insert_pheno_meta,
                                 std::to_string(pheno_meta_idx), field_id,
                                 pheno);
            ++pheno_meta_idx;
            return pheno_meta_idx - 1;
        }
    }
    else
    {
        pheno_id[field_id][pheno] = pheno_meta_idx;
        update_pheno_meta_db(db, insert_pheno_meta,
                             std::to_string(pheno_meta_idx), field_id, pheno);
        ++pheno_meta_idx;
        return pheno_meta_idx - 1;
    }
}

void update_pheno_db(sqlite3* db, sqlite3_stmt* insert_pheno,
                     const std::string& sample_id, const std::string& pheno_id,
                     const std::string& instance)
{
    sqlite3_bind_text(insert_pheno, 1, sample_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(insert_pheno, 2, pheno_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(insert_pheno, 3, instance.c_str(), -1, SQLITE_TRANSIENT);
    int status = sqlite3_step(insert_pheno);
    if (status != SQLITE_DONE || status == SQLITE_ERROR
        || status == SQLITE_BUSY)
    {
        std::string errorMessage(sqlite3_errmsg(db));
        throw std::runtime_error("Error: Insert failed: " + errorMessage + " ("
                                 + std::to_string(status) + ")");
    }
    sqlite3_clear_bindings(insert_pheno);
    sqlite3_reset(insert_pheno);
}

void print_progress(signed long long cur_loc, signed long long length,
                    double& prev_percentage)
{
    double cur_progress =
        (static_cast<double>(cur_loc) / static_cast<double>(length)) * 100.0;
    if (cur_progress - prev_percentage > 0.01)
    {
        fprintf(stderr, "\rProcessing %03.2f%%", cur_progress);
        prev_percentage = cur_progress;
    }
    else if (prev_percentage >= 100.0)
    {
        fprintf(stderr, "\rProcessing %03.2f%%", 100.0);
    }
}

void insert_sample_db(sqlite3* db, sqlite3_stmt* insert_sample,
                      const std::string& sample_id)
{
    sqlite3_bind_text(insert_sample, 1, sample_id.c_str(), -1,
                      SQLITE_TRANSIENT);
    sqlite3_bind_text(insert_sample, 2, (sample_id.at(0) == '-') ? "1" : "0",
                      -1, SQLITE_TRANSIENT);
    int status = sqlite3_step(insert_sample);
    if (status != SQLITE_DONE || status == SQLITE_ERROR
        || status == SQLITE_BUSY)
    {
        std::string errorMessage(sqlite3_errmsg(db));
        throw std::runtime_error("Error: Insert failed: " + errorMessage + " ("
                                 + std::to_string(status) + ")");
    }
    sqlite3_clear_bindings(insert_sample);
    sqlite3_reset(insert_sample);
}

signed long long get_file_length(std::ifstream& pheno_file)
{
    pheno_file.seekg(0, pheno_file.end);
    signed long long file_length = pheno_file.tellg();
    pheno_file.clear();
    pheno_file.seekg(0, pheno_file.beg);
    return file_length;
}
void load_phenotype(sqlite3* db, std::unordered_set<std::string>& fields,
                    const std::vector<std::string> pheno_names,
                    const bool danger)
{
    std::string insert_statement = "INSERT INTO SAMPLE(ID, DropOut) "
                                   "VALUES(@S,@I)";
    sqlite3_stmt* insert_sample;
    sqlite3_prepare_v2(db, insert_statement.c_str(), -1, &insert_sample,
                       nullptr);
    std::string insert_pheno_statement = "INSERT INTO PHENOTYPE"
                                         "(ID, Instance, PhenoID) "
                                         "VALUES(@S,@I,@P)";
    sqlite3_stmt* insert_pheno;
    sqlite3_prepare_v2(db, insert_pheno_statement.c_str(), -1, &insert_pheno,
                       nullptr);
    std::string insert_pheno_meta_statement = "INSERT INTO  PHENO_META"
                                              "(ID, FieldID, Pheno) "
                                              "VALUES(@S,@I,@P)";
    sqlite3_stmt* insert_meta;
    sqlite3_prepare_v2(db, insert_pheno_meta_statement.c_str(), -1,
                       &insert_meta, nullptr);
    char* zErrMsg = nullptr;
    if (danger)
    {
        sqlite3_exec(db, "PRAGMA synchronous = OFF", nullptr, nullptr,
                     &zErrMsg);
        sqlite3_exec(db, "PRAGMA journal_mode = MEMORY", nullptr, nullptr,
                     &zErrMsg);
    }


    std::unordered_map<std::string, std::unordered_map<std::string, size_t>>
        pheno_id_dict;
    size_t pheno_meta_idx = 0;
    unsigned long long na_entries = 0;
    unsigned long long counts = 0;
    std::string field_id, instance_num;
    sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, &zErrMsg);
    for (auto&& pheno : pheno_names)
    {
        std::ifstream pheno_file(pheno.c_str());
        if (!pheno_file.is_open())
        {
            throw std::runtime_error(
                "Error: Cannot open phenotype file: " + pheno
                + ". Please check you have the correct input");
        }
        std::string line;
        std::string sql;
        // there is a header
        const signed long long file_length = get_file_length(pheno_file);
        std::cerr
            << std::endl
            << "============================================================"
            << std::endl;
        // process the header
        // should be the Field ID and Instance number
        size_t id_idx = 0;
        // pheno meta let us know for this column, what's the Field ID and
        // what's the instance
        std::getline(pheno_file, line);
        std::vector<std::string> token = misc::split(line, "\t");
        std::vector<pheno_info> phenotype_meta =
            get_pheno_meta(pheno, token, fields, id_idx);
        const size_t num_pheno = phenotype_meta.size();
        std::cerr << "Start processing phenotype file with " << num_pheno
                  << " entries (" << pheno << ")" << std::endl;
        double prev_percentage = 0;
        fprintf(stderr, "\rProcessing %03.2f%%", 0.00);
        while (std::getline(pheno_file, line))
        {
            misc::trim(line);
            if (line.empty()) continue;
            print_progress(pheno_file.tellg(), file_length, prev_percentage);
            // Tab Delim
            token = misc::split(line, "\t");
            if (token.size() != num_pheno)
            {
                throw std::runtime_error(
                    "Error: Undefined Phenotype file"
                    "format! File is expected to have exactly "
                    + misc::to_string(num_pheno) + " columns. Line has :"
                    + std::to_string(token.size()) + " column(s)\n");
            }
            for (size_t i = 0; i < num_pheno; ++i)
            {
                if (token[i] == "NA")
                {
                    ++na_entries;
                    continue;
                }
                else if (i == id_idx)
                {
                    insert_sample_db(db, insert_sample, token[i]);
                    continue;
                }
                else if (phenotype_meta[i].first == "NA")
                {
                    // skipped field
                    continue;
                }
                // check meta
                size_t pheno_id = get_phenotype_id(
                    db, insert_meta, pheno_id_dict, pheno_meta_idx,
                    phenotype_meta[i].first, token[i]);
                update_pheno_db(db, insert_pheno, token[id_idx],
                                std::to_string(pheno_id),
                                phenotype_meta[i].second);
                ++counts;
            }
        }
        pheno_file.close();
    }
    sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
    fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
    std::cerr << "Start building indexs" << std::endl;
    std::string sql = "CREATE INDEX 'PHENOTYPE_INDEX' ON 'PHENOTYPE'  ('ID')";
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    sql = "CREATE INDEX 'PHENOTYPE_INSTANCE_INDEX' ON 'PHENOTYPE'  "
          "('Instance', 'PhenoID')";
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    sql = "CREATE INDEX 'PHENOTYPE_FULL_INDEX' ON 'PHENOTYPE'  "
          "('Instance', 'PhenoID', 'ID')";
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    sql = "CREATE INDEX 'PHENOTYPE_NO_INSTANCE_INDEX' ON 'PHENOTYPE'  "
          "('PhenoID', 'ID')";
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    sql = "CREATE INDEX 'PHENOTYPE_META_INDEX' ON 'PHENO_META'  "
          " ('FieldID', 'ID')";
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    sql = "CREATE INDEX 'PHENOTYPE_META_LITE_INDEX' ON 'PHENO_META'  "
          " ('FieldID')";
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);

    std::cerr << "A total of " << counts << " entries entered into database"
              << std::endl;
    if (na_entries)
    { std::cerr << "With " << na_entries << " NA entries" << std::endl; }
}


void usage()
{

    fprintf(stderr, " UK Biobank Phenotype Processing\n");
    fprintf(stderr, " Sam Choi\n");
    fprintf(stderr, " v0.2.0 ( 2019-10-16 )\n");
    fprintf(stderr, " ==============================\n");
    fprintf(stderr, " This program will process the UK biobank Phenotype\n");
    fprintf(stderr, " information and generate a SQLite data base\n");
    fprintf(stderr,
            " Usage: ukb_process -d <Data showcase> -c <Code showcase> \\\n");
    fprintf(stderr, "                    -p <Phenotype> -o <Output>\n");
    fprintf(stderr,
            "    -d | --data     Data showcase file. Can be found here:\n");
    fprintf(
        stderr,
        "                    "
        "http://biobank.ndph.ox.ac.uk/~bbdatan/Data_Dictionary_Showcase.csv\n");
    fprintf(
        stderr,
        "    -c | --code     Data coding information. Can be found here:\n");
    fprintf(stderr,
            "                    "
            "http://biobank.ndph.ox.ac.uk/~bbdatan/Codings_Showcase.csv\n");
    fprintf(stderr, "    -p | --pheno    UK Biobank Phenotype file\n");
    fprintf(stderr, "    -o | --out      Name of the generated database\n");
    fprintf(stderr,
            "    -D | --danger   Enable optioned that speed up processing\n");
    fprintf(stderr,
            "                    May generate corrupted database file if\n");
    fprintf(stderr, "                    server is unstable\n");
    fprintf(stderr, "    -m | --memory   Cache memory, default 1024byte\n");
    fprintf(stderr, "    -r | --replace  Replace existing ukb database file\n");
    fprintf(stderr, "    -h | --help     Display this help message\n\n\n");
}
int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        usage();
        return -1;
    }
    static const char* optString = "d:c:p:o:m:rDh?";
    static const struct option longOpts[] = {
        {"data", required_argument, nullptr, 'd'},
        {"code", required_argument, nullptr, 'c'},
        {"pheno", required_argument, nullptr, 'p'},
        {"out", required_argument, nullptr, 'o'},
        {"memory", required_argument, nullptr, 'm'},
        {"replace", no_argument, nullptr, 'r'},
        {"danger", no_argument, nullptr, 'D'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0}};
    int longIndex = 0;
    int opt = 0;
    opt = getopt_long(argc, argv, optString, longOpts, &longIndex);
    std::string data_showcase, code_showcase, pheno_name, out_name,
        memory = "1024";
    bool replace = false, danger = false;
    while (opt != -1)
    {
        switch (opt)
        {
        case 'd': data_showcase = optarg; break;
        case 'm': memory = optarg; break;
        case 'D': danger = true; break;
        case 'c': code_showcase = optarg; break;
        case 'p': pheno_name = optarg; break;
        case 'o': out_name = optarg; break;
        case 'r': replace = true; break;
        case 'h':
        case '?': usage(); return 0;
        default:
            throw "Undefined operator, please use --help for more "
                  "information!";
        }
        opt = getopt_long(argc, argv, optString, longOpts, &longIndex);
    }
    bool error = false;
    if (data_showcase.empty())
    {
        error = true;
        std::cerr << "Error: You must provide the data showcase csv file!"
                  << std::endl;
    }
    if (code_showcase.empty())
    {
        error = true;
        std::cerr << "Error: You must provide the code showcase csv file!"
                  << std::endl;
    }
    if (pheno_name.empty())
    {
        error = true;
        std::cerr << "Error: You must provide the phenotype!" << std::endl;
    }
    if (out_name.empty())
    {
        error = true;
        std::cerr << "Error: You must provide output prefix!" << std::endl;
    }
    if (error)
    {
        std::cerr << "Please check you have all the required input!"
                  << std::endl;
        return -1;
    }
    std::string db_name = out_name + ".db";
    sqlite3* db;
    if (misc::file_exists(db_name))
    {
        // emit warning and delete file
        if (!replace)
        {
            std::cerr << "Error: Database file exists: " + db_name << std::endl;
            std::cerr << "       Use --replace to replace it" << std::endl;
            return -1;
        }
        std::remove(db_name.c_str());
    }
    int rc = sqlite3_open(db_name.c_str(), &db);
    if (rc)
    {
        std::cerr << "Cannot open database: " << db_name << std::endl;
        return -1;
    }
    else
    {
        std::cerr << "Opened database: " << db_name << std::endl;
    }

    create_tables(db, memory);
    std::unordered_set<std::string> included_fields;
    std::vector<std::string> pheno_names = misc::split(pheno_name, ",");
    load_phenotype(db, included_fields, pheno_names, danger);
    load_data(db, included_fields, data_showcase);
    load_code(db, code_showcase);
    sqlite3_close(db);
    return 0;
}
