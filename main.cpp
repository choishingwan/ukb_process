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
#include <unordered_set>
#include <unordered_map>
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
          "FOREIGN KEY (Coding) REFERENCES CODE(ID));";
    rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    else
    {
        fprintf(stdout, "Table:DATA_META created successfully\n");
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

    while (std::getline(code, line)) {
        misc::trim(line);
        if (line.empty()) continue;
        double cur_progress = (static_cast<double>(code.tellg())
                               / static_cast<double>(file_length))
                              * 100.0;
        // progress bar can be slow when permutation + thresholding is used due
        // to the huge amount of processing required
        if (cur_progress - prev_percentage > 0.01) {
            fprintf(stderr, "\rProcessing %03.2f%%", cur_progress);
            prev_percentage = cur_progress;
        }

        if (prev_percentage >= 100.0) {
            fprintf(stderr, "\rProcessing %03.2f%%", 100.0);
        }
        // CSV input
        token = misc::csv_split(line);
        if (token.size() != 3) {
            std::string error_message =
                "Error: Undefined Code Showcase "
                "format! File is expected to have exactly 3 columns.\n"
                + line;
            throw std::runtime_error(error_message);
        }
        for (size_t i = 3; i < token.size(); ++i) {
            token[2] = token[2] + "," + token[i];
        }
        // clean up the string
        token[2].erase(std::remove(token[2].begin(), token[2].end(), '\"'),
                       token[2].end());
        token[2] = "\"" + token[2] + "\"";
        if (id.find(token[0]) == id.end()) {
            // ADD this into CODE table
            sqlite3_bind_text(code_stat, 0, token[0].c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_step(code_stat);
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
        sqlite3_step(code_meta_stat);
        sqlite3_clear_bindings(code_meta_stat);
        sqlite3_reset(code_meta_stat);
    }
    code.close();
    sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
    sqlite3_exec(db, "CREATE INDEX 'CODE_META_Index' ON 'CODE_META' ('ID')",
                 nullptr, nullptr, &zErrMsg);
    fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
}

void load_data(sqlite3* db, const std::string& data_showcase)
{
    std::ifstream data(data_showcase.c_str());
    if (!data.is_open()) {
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
        "Instances, Array, Coding) "
        "VALUES(@CATEGORY,@FIELDID,@FIELD,@PARTICIPANTS,@ITEM,@STABILITY,"
        "@VALUETYPE,@UNITS,@ITEMTYPE,@STRATA,@SEXED,@INSTANCES,@ARRAY,@CODING)";
    sqlite3_prepare_v2(db, data_statement.c_str(), -1, &dat_stat, nullptr);
    sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, &zErrMsg);

    while (std::getline(data, line)) {
        misc::trim(line);
        if (line.empty()) continue;
        double cur_progress = (static_cast<double>(data.tellg())
                               / static_cast<double>(file_length))
                              * 100.0;
        // progress bar can be slow when permutation + thresholding is used due
        // to the huge amount of processing required
        if (cur_progress - prev_percentage > 0.01) {
            fprintf(stderr, "\rProcessing %03.2f%%", cur_progress);
            prev_percentage = cur_progress;
        }

        if (prev_percentage >= 100.0) {
            fprintf(stderr, "\rProcessing %03.2f%%", 100.0);
        }
        // CSV input
        token = misc::csv_split(line);
        if (token.size() != 17) {
            std::string error_message =
                "Error: Undefined Data Showcase "
                "format! File is expected to have exactly 17 columns.\n"
                + line;
            throw std::runtime_error(error_message);
        }
        for (size_t i = 1; i < 15; ++i) {
            // we skip the first one and last 2 as they are not as useful
            // can always retrieve those using data showcase
            if (i == 3 || (i >= 6 && i <= 11)) {
                token[i].erase(
                    std::remove(token[i].begin(), token[i].end(), '\"'),
                    token[i].end());
                if (token[i] != "NULL") token[i] = "\"" + token[i] + "\"";
            }
            sqlite3_bind_text(dat_stat, static_cast<int>(i), token[i].c_str(),
                              -1, SQLITE_TRANSIENT);
        }
        sqlite3_step(dat_stat);
        sqlite3_clear_bindings(dat_stat);
        sqlite3_reset(dat_stat);
    }
    data.close();
    sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
    fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
    sqlite3_exec(db, "CREATE INDEX 'DATA_Index' ON 'DATA_META' ('FieldID')",
                 nullptr, nullptr, &zErrMsg);
}


void load_phenotype(sqlite3* db, const std::string& pheno_name,
                    const bool danger)
{
    std::ifstream pheno(pheno_name.c_str());
    if (!pheno.is_open()) {
        std::string error_message =
            "Error: Cannot open phenotype file: " + pheno_name
            + ". Please check you have the correct input";
        throw std::runtime_error(error_message);
    }
    std::string line;
    std::string sql;
    char* zErrMsg = nullptr;
    // there is a header
    pheno.seekg(0, pheno.end);
    auto file_length = pheno.tellg();
    pheno.clear();
    pheno.seekg(0, pheno.beg);
    std::getline(pheno, line);
    std::cerr << std::endl
              << "============================================================"
              << std::endl;
    // process the header
    // should be the Field ID and Instance number
    typedef std::pair<std::string, std::string> pheno_info;
    size_t id_idx = 0;
    std::vector<pheno_info> phenotype_meta;
    std::vector<std::string> token = misc::split(line, "\t");
    std::vector<std::string> subtoken;
    //std::unordered_set<std::string> pheno_id;
    std::unordered_map<std::string, sqlite3_stmt*> pheno_statements;
    int rc;
    for (size_t i = 0; i < token.size(); ++i) {
        if (token[i] == "f.eid" || token[i] == "\"f.eid\"") {
            phenotype_meta.emplace_back(std::make_pair("0", "0"));
            id_idx = i;
        }
        else
        {
            // we don't care about the array index
            subtoken = misc::split(token[i], ".");
            if (subtoken.size() != 4) {
                std::string error_message = "Error: We expect all Field ID "
                                            "from the phenotype to have the "
                                            "following format: f.x.x.x: "
                                            + token[i];
                throw std::runtime_error(error_message);
            }
            //if(pheno_id.find(subtoken[1])==pheno_id.end()){
            if(pheno_statements.find(subtoken[1])==pheno_statements.end()){
                //pheno_id.insert(subtoken[1]);
                sql = "CREATE TABLE f"+subtoken[1]+"("
                      "SampleID INT NOT NULL,"
                      "Instance INT NOT NULL,"
                      "Pheno TEXT,"
                      "FOREIGN KEY (SampleID) REFERENCES SAMPLE(ID)"
                      ");";
                rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
                if (rc != SQLITE_OK) {
                    fprintf(stderr, "SQL error: %s\n", zErrMsg);
                    sqlite3_free(zErrMsg);
                }
                std::string cur_statement =
                    "INSERT INTO f"+subtoken[1]+"(SampleID, Instance, Pheno) "
                    "VALUES(@S,@I,@P)";

                sqlite3_stmt* cur_stat;
                sqlite3_prepare_v2(db, cur_statement.c_str(), -1, &cur_stat, nullptr);
                pheno_statements[subtoken[1]] = cur_stat;

            }
            phenotype_meta.emplace_back(
                std::make_pair(subtoken[1], subtoken[2]));
        }
    }
    const size_t num_pheno = phenotype_meta.size();
    std::cerr << "Start processing phenotype file with " << num_pheno
              << " entries" << std::endl;


    double prev_percentage = 0;
    if (danger) {
        sqlite3_exec(db, "PRAGMA synchronous = OFF", nullptr, nullptr, &zErrMsg);
        sqlite3_exec(db, "PRAGMA journal_mode = MEMORY", nullptr, nullptr, &zErrMsg);
    }
    sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, &zErrMsg);
    fprintf(stderr, "\rProcessing %03.2f%%", 0.00);
    size_t count = 0,num_line=0;
    while (std::getline(pheno, line)) {

        misc::trim(line);
        if (line.empty()) continue;
        double cur_progress = (static_cast<double>(pheno.tellg())
                               / static_cast<double>(file_length))
                              * 100.0;
        // progress bar can be slow when permutation + thresholding is used due
        // to the huge amount of processing required
        if (cur_progress - prev_percentage > 0.01) {
            fprintf(stderr, "\rProcessing %03.2f%%", cur_progress);
            prev_percentage = cur_progress;
        }

        if (prev_percentage >= 100.0) {
            fprintf(stderr, "\rProcessing %03.2f%%", 100.0);
        }
        // Tab Delim
        token = misc::split(line, "\t");
        if (token.size() != num_pheno) {
            std::string error_message =
                "Error: Undefined Phenotype file"
                "format! File is expected to have exactly "
                + misc::to_string(num_pheno) + " columns. Line has :"
                + std::to_string(token.size()) + " column(s)\n";
            throw std::runtime_error(error_message);
        }
        num_line++;
        for (size_t i = 0; i < num_pheno; ++i) {
            if (token[i] == "NA" || i == id_idx) continue;
            auto &cur_stat = pheno_statements[phenotype_meta[i].first.c_str()];
            // sample ID
            sqlite3_bind_text(cur_stat, 1, token[id_idx].c_str(), -1,
                              SQLITE_TRANSIENT);
            // Instance
            sqlite3_bind_text(cur_stat, 2, phenotype_meta[i].second.c_str(),
                              -1, SQLITE_TRANSIENT);
            // phenotype
            if (token[i].front() != '\"') token[i] = "\"" + token[i];
            if (token[i].back() != '\"') token[i] = token[i] + "\"";
            sqlite3_bind_text(cur_stat, 3, token[i].c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_step(cur_stat);
            sqlite3_clear_bindings(cur_stat);
            sqlite3_reset(cur_stat);
            /*
            sql = "INSERT INTO f"+
                    phenotype_meta[i].first+
                    "(SampleID, Instance, Pheno) "
                    "VALUES("+token[id_idx]+","+
                    phenotype_meta[i].second+","+
                    token[i]+");";
            rc = sqlite3_exec(db, sql.c_str(), callback, nullptr, &zErrMsg);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
            }
            */
            count++;
        }
    }
    pheno.close();
    // currently this should be the most useful index. Maybe add some more if
    // we can figure out their use case
    sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
    for(auto pheno : pheno_statements){
    //for(auto pheno : pheno_id){
        sql = std::string("CREATE INDEX 'f"+pheno.first+"_Index' ON 'f"+pheno.first+"' ('Instance')");
        //sql = std::string("CREATE INDEX 'f"+pheno+"_Index' ON 'f"+pheno+"' ('Instance')");
        sqlite3_exec(
            db,
            sql.c_str(),
            nullptr, nullptr, &zErrMsg);
    }
    fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
    size_t na = num_line*num_pheno-count;
    std::cerr << "A total of " << count << " entries entered into database" << std::endl;
    if(na){
        std::cerr << "With " << na << " NA entries" << std::endl;
    }
}
void usage(){

    fprintf(stderr, " UK Biobank Phenotype Processing\n");
    fprintf(stderr, " Sam Choi\n");
    fprintf(stderr, " v0.1.0 ( 2019-06-04 )\n");
    fprintf(stderr, " ==============================\n");
    fprintf(stderr, " This program will process the UK biobank Phenotype\n");
    fprintf(stderr, " information and generate a SQLite data base\n");
    fprintf(stderr, " Usage: ukb_process -d <Data showcase> -c <Code showcase> \\\n");
    fprintf(stderr, "                    -p <Phenotype> -o <Output>\n");
    fprintf(stderr, "    -d | --data     Data showcase file. Can be found here:\n");
    fprintf(stderr, "                    http://biobank.ndph.ox.ac.uk/~bbdatan/Data_Dictionary_Showcase.csv\n");
    fprintf(stderr, "    -c | --code     Data coding information. Can be found here:\n");
    fprintf(stderr, "                    http://biobank.ndph.ox.ac.uk/~bbdatan/Codings_Showcase.csv\n");
    fprintf(stderr, "    -p | --pheno    UK Biobank Phenotype file\n");
    fprintf(stderr, "    -o | --out      Name of the generated database\n");
    fprintf(stderr, "    -D | --danger   Enable optioned that speed up processing\n");
    fprintf(stderr, "                    May generate corrupted database file if\n");
    fprintf(stderr, "                    server is unstable\n");
    fprintf(stderr, "    -m | --memory   Cache memory, default 1024byte\n");
    fprintf(stderr, "    -r | --replace  Replace existing ukb database file\n");
    fprintf(stderr, "    -h | --help     Display this help message\n\n\n");
}
int main(int argc, char* argv[])
{
    if(argc < 2){
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
    while (opt != -1) {
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
        case '?':
            usage();
            return 0;
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
    if (misc::file_exists(db_name)) {
        // emit warning and delete file
        if (!replace) {
            std::cerr << "Error: Database file exists: " + db_name << std::endl;
            std::cerr << "       Use --replace to replace it" << std::endl;
            return -1;
        }
        std::remove(db_name.c_str());
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

    create_tables(db, memory);
    load_code(db, code_showcase);
    load_data(db, data_showcase);
    load_phenotype(db, pheno_name, danger);
    sqlite3_close(db);
    return 0;
}
