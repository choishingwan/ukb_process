#include "misc.hpp"
#include "sql.h"
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

signed long long get_file_length(std::ifstream& pheno_file)
{
    pheno_file.seekg(0, pheno_file.end);
    signed long long file_length = pheno_file.tellg();
    pheno_file.clear();
    pheno_file.seekg(0, pheno_file.beg);
    return file_length;
}

void load_code(sqlite3* db, const std::string& code_showcase)
{
    std::ifstream code(code_showcase.c_str());
    if (!code.is_open())
    {
        throw std::runtime_error("Error: Cannot open code showcase file: "
                                 + code_showcase
                                 + ". Please check you have the correct input");
    }
    std::string line;
    // there is a header
    auto&& file_length = get_file_length(code);
    std::cerr << std::endl
              << "============================================================"
              << std::endl;
    std::cerr << "Header line of code showcase: " << std::endl;
    std::getline(code, line);
    std::cerr << line << std::endl;
    double prev_percentage = 0;
    std::vector<std::string> token;
    std::unordered_set<std::string> id;
    SQL code_table("CODE", db);
    SQL code_meta("CODE_META", db);
    code_table.create_table("CREATE TABLE CODE("
                            "ID INT PRIMARY KEY NOT NULL);");
    code_meta.create_table("CREATE TABLE CODE_META("
                           "ID INT,"
                           "Value INT NOT NULL,"
                           "Meaning TEXT,"
                           "FOREIGN KEY (ID) REFERENCES CODE(ID));");
    code_table.prep_statement("INSERT INTO CODE(ID) VALUES(@ID)");
    code_meta.prep_statement(
        "INSERT INTO CODE_META(ID, Value, Meaning) VALUES(@ID,@V, @M)");
    char* zErrMsg = nullptr;
    sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, &zErrMsg);
    while (std::getline(code, line))
    {
        misc::trim(line);
        if (line.empty()) continue;
        print_progress(code.tellg(), file_length, prev_percentage);
        // CSV input
        token = misc::csv_split(line);
        if (token.size() != 3)
        {
            throw std::runtime_error(
                "Error: Undefined Code Showcase "
                "format! File is expected to have exactly 3 columns.\n"
                + line);
        }
        // some fields got comma in it, need to handle it
        for (size_t i = 3; i < token.size(); ++i)
        { token[2] = token[2] + "," + token[i]; }
        // clean up the string, remove all redundent "
        token[2].erase(std::remove(token[2].begin(), token[2].end(), '\"'),
                       token[2].end());
        // add the " back as a protection
        token[2] = "\"" + token[2] + "\"";
        if (id.find(token[0]) == id.end())
        {
            // ADD this into CODE table
            code_table.run_statement(std::vector<std::string> {token[0]});
            id.insert(token[0]);
        }
        code_meta.run_statement(token);
    }
    code.close();
    sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
    code_meta.create_index("CODE_META_VALUE_INDEX",
                           std::vector<std::string> {"ID", "Value"});
    code_meta.create_index("CODE_META_INDEX", std::vector<std::string> {"ID"});
    fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
}

void load_data(sqlite3* db,
               const std::unordered_set<std::string>& included_fields,
               const std::string& data_showcase)
{
    std::cerr << "Total " << included_fields.size() << " fields to be included"
              << std::endl;
    std::ifstream data(data_showcase.c_str());
    if (!data.is_open())
    {
        throw std::runtime_error("Error: Cannot open data showcase file: "
                                 + data_showcase
                                 + ". Please check you have the correct input");
    }
    std::string line;
    // there is a header
    auto&& file_length = get_file_length(data);
    std::cerr << std::endl
              << "============================================================"
              << std::endl;
    std::cerr << "Header line of data showcase: " << std::endl;
    std::getline(data, line);
    std::cerr << line << std::endl;
    double prev_percentage = 0;
    std::vector<std::string> token;
    SQL data_meta("DATA_META", db);
    data_meta.create_table("CREATE TABLE DATA_META("
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
                           "FOREIGN KEY (Coding) REFERENCES CODE(ID));");
    data_meta.prep_statement(
        "INSERT INTO DATA_META(Category, FieldID, Field, Participants, "
        "Items, Stability, ValueType, Units, ItemType, Strata, Sexed, "
        "Instances, Array, Coding, Included) "
        "VALUES(@CATEGORY,@FIELDID,@FIELD,@PARTICIPANTS,@ITEM,@STABILITY,"
        "@VALUETYPE,@UNITS,@ITEMTYPE,@STRATA,@SEXED,@INSTANCES,@ARRAY,@CODING, "
        "@INCLUDED)");
    char* zErrMsg = nullptr;
    sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, &zErrMsg);

    while (std::getline(data, line))
    {
        misc::trim(line);
        if (line.empty()) continue;
        print_progress(data.tellg(), file_length, prev_percentage);
        // CSV input
        token = misc::csv_split(line);
        if (token.size() != 17)
        {
            throw std::runtime_error(
                "Error: Undefined Data Showcase "
                "format! File is expected to have exactly 17 columns.\n"
                + line);
        }
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
        }
        bool field_included =
            (included_fields.find(token[2]) == included_fields.end());
        token[15] = field_included ? "0" : "1";
        data_meta.run_statement(token, 16, 1);
    }
    data.close();
    sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
    fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
    data_meta.create_index("DATA_INDEX", std::vector<std::string> {"FieldID"});
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


size_t get_phenotype_id(
    SQL& pheno_meta,
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
            pheno_meta.run_statement(std::vector<std::string> {
                std::to_string(pheno_meta_idx), field_id, pheno});
            ++pheno_meta_idx;
            return pheno_meta_idx - 1;
        }
    }
    else
    {
        pheno_id[field_id][pheno] = pheno_meta_idx;
        pheno_meta.run_statement(std::vector<std::string> {
            std::to_string(pheno_meta_idx), field_id, pheno});
        ++pheno_meta_idx;
        return pheno_meta_idx - 1;
    }
}


void load_phenotype(sqlite3* db, std::unordered_set<std::string>& fields,
                    const std::vector<std::string> pheno_names,
                    const bool danger)
{
    SQL phenotype("PHENOTYPE", db);
    SQL pheno_meta("PHENO_META", db);
    SQL participants("PARTICIPANT", db);
    phenotype.create_table("CREATE TABLE PHENOTYPE("
                           "ID INT NOT NULL,"
                           "Instance INT NOT NULL,"
                           "PhenoID INT NOT NULL,"
                           "FOREIGN KEY (ID) REFERENCES PARTICIPANT(ID),"
                           "FOREIGN KEY (PhenoID) REFERENCES PHENO_META(ID));");
    pheno_meta.create_table(
        "CREATE TABLE PHENO_META("
        "ID INT PRIMARY KEY NOT NULL,"
        "FieldID INT NOT NULL,"
        "Pheno TEXT NOT NULL,"
        "FOREIGN KEY (FieldID) REFERENCES DATA_META(FieldID));");
    // drop out shouldn't even be stored in the database
    participants.create_table("CREATE TABLE PARTICIPANT("
                              "ID INT PRIMARY KEY NOT NULL);");
    participants.prep_statement("INSERT INTO PARTICIPANT(ID) "
                                "VALUES(@S)");
    phenotype.prep_statement("INSERT INTO PHENOTYPE"
                             "(ID, Instance, PhenoID) "
                             "VALUES(@S,@I,@P)");
    pheno_meta.prep_statement("INSERT INTO  PHENO_META"
                              "(ID, FieldID, Pheno) "
                              "VALUES(@S,@I,@P)");
    char* zErrMsg = nullptr;
    if (danger)
    {
        sqlite3_exec(db, "PRAGMA synchronous = OFF", nullptr, nullptr,
                     &zErrMsg);
        sqlite3_exec(db, "PRAGMA journal_mode = MEMORY", nullptr, nullptr,
                     &zErrMsg);
    }

    // this is easy, but ugly
    std::unordered_map<std::string, std::unordered_map<std::string, size_t>>
        pheno_id_dict;
    std::unordered_set<std::string> processed_sample;
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
                else if (i == id_idx
                         && processed_sample.find(token[i])
                                == processed_sample.end())
                {
                    processed_sample.insert(token[i]);
                    participants.run_statement(token, i + 1, i);
                    continue;
                }
                else if (phenotype_meta[i].first == "NA")
                {
                    continue;
                }
                // check meta
                size_t pheno_id =
                    get_phenotype_id(pheno_meta, pheno_id_dict, pheno_meta_idx,
                                     phenotype_meta[i].first, token[i]);
                phenotype.run_statement(std::vector<std::string> {
                    token[id_idx], std::to_string(pheno_id),
                    phenotype_meta[i].second});
                ++counts;
            }
        }
        fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
        pheno_file.close();
    }
    sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
    std::cerr << "Start building indexs" << std::endl;
    phenotype.create_index("PHENOTYPE_INDEX", std::vector<std::string> {"ID"});
    phenotype.create_index("PHENOTYPE_INSTANCE_INDEX",
                           std::vector<std::string> {"Instance", "PhenoID"});
    phenotype.create_index(
        "PHENOTYPE_FULL_INDEX",
        std::vector<std::string> {"Instance", "PhenoID", "ID"});
    phenotype.create_index("PHENOTYPE_NO_INSTANCE_INDEX",
                           std::vector<std::string> {"PhenoID", "ID"});
    pheno_meta.create_index("PHENOTYPE_META_INDEX",
                            std::vector<std::string> {"FieldID", "ID"});
    pheno_meta.create_index("PHENOTYPE_META_LITE_INDEX",
                            std::vector<std::string> {"FieldID"});
    pheno_meta.create_index(
        "PHENOTYPE_META_FULL_INDEX",
        std::vector<std::string> {"FieldID", "Pheno", "ID"});
    participants.create_index("PARTICIPANT", std::vector<std::string> {"ID"});
    std::cerr << "A total of " << counts << " entries entered into database"
              << std::endl;
    if (na_entries)
    { std::cerr << "With " << na_entries << " NA entries" << std::endl; }
}

void load_provider(sqlite3* db)
{
    SQL gp_provider("gp_provider", db);
    gp_provider.create_table("CREATE TABLE gp_provider(ID PRIMARY KEY INT NOT "
                             "NULL, NAME TEXT NOT NULL );");
    gp_provider.execute_sql("insert into HEALTH_PROVIDER (ID, PROVIDER) "
                            "VALUES(1, \"England(Vision)\")");
    gp_provider.execute_sql("insert into HEALTH_PROVIDER (ID, PROVIDER) "
                            "VALUES(2, \"Scotland\")");
    gp_provider.execute_sql("insert into HEALTH_PROVIDER (ID, PROVIDER) "
                            "VALUES(3, \"England(TPP)\")");
    gp_provider.execute_sql("insert into HEALTH_PROVIDER (ID, PROVIDER) "
                            "VALUES(4, \"Wales\")");
    gp_provider.create_index("PROVIDER_INDEX", std::vector<std::string> {"ID"});
}
void load_gp(sqlite3* db, const std::string& gp_record, const std::string& drug)
{
    if (gp_record.empty() && drug.empty()) return;
    load_provider(db);
    if (!gp_record.empty())
    {
        std::ifstream gp_file(gp_record.c_str());
        if (!gp_file.is_open())
        {
            throw std::runtime_error(
                "Error: Cannot open primary care record: " + gp_record
                + ". Please check you have the correct input");
        }
        std::string line;
        // there is a header
        auto&& file_length = get_file_length(gp_file);
        std::cerr
            << std::endl
            << "============================================================"
            << std::endl;
        std::cerr << "Header line of primary care record: " << std::endl;
        std::getline(gp_file, line);
        std::cerr << line << std::endl;
        double prev_percentage = 0;
        std::vector<std::string> token;
        SQL gp_clinical("gp_clinical", db);
        gp_clinical.create_table(
            "CREATE TABLE gp_clinical("
            "ID INT NOT NULL,"
            "data_provider INT NOT NULL,"
            "date_event TEXT NOT NULL, "
            "Read2 TEXT, "
            "Read3 TEXT, "
            "Value1 TEXT,"
            "Value2 TEXT,"
            "Value3 TEXT, "
            "FOREIGN KEY (ID) REFERENCES PARTICIPANT(ID),"
            "FOREIGN KEY (data_provider) REFERENCES gp_provider(ID));");
        gp_clinical.prep_statement(
            "INSERT INTO DATA_META(ID, data_provider, date_event, Read2, "
            "Read3, Value1, Value2, Value3) "
            "VALUES(@ID,@PROVIDER,@DATE,@READ2,@READ3,@VALUE1,"
            "@VALUE2,@VALUE3)");
        char* zErrMsg = nullptr;
        sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, &zErrMsg);
        while (std::getline(gp_file, line))
        {
            misc::trim(line);
            if (line.empty()) continue;
            print_progress(gp_file.tellg(), file_length, prev_percentage);
            // CSV input
            token = misc::csv_split(line);
            if (token.size() != 8)
            {
                throw std::runtime_error(
                    "Error: Undefined primary care record "
                    "format! File is expected to have exactly 8 columns.\n"
                    + line);
            }
            gp_clinical.run_statement(token);
        }
        gp_file.close();
        sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
        fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
        gp_clinical.create_index("gp_clinical_read2",
                                 std::vector<std::string> {"Read2", "ID"});
        gp_clinical.create_index("gp_clinical_read3",
                                 std::vector<std::string> {"Read3", "ID"});
        gp_clinical.create_index(
            "gp_clinical_reads",
            std::vector<std::string> {"Read3", "Read2", "ID"});
        gp_clinical.create_index("gp_clinical_date",
                                 std::vector<std::string> {"date_event", "ID"});
        gp_clinical.create_index(
            "gp_clinical_reads_date",
            std::vector<std::string> {"Read3", "Read2", "date_event", "ID"});
    }
    if (!drug.empty())
    {
        SQL gp_drug("gp_scripts", db);
        std::ifstream drug_file(drug.c_str());
        if (!drug_file.is_open())
        {
            throw std::runtime_error(
                "Error: Cannot open prescription record: " + drug
                + ". Please check you have the correct input");
        }
        std::string line;
        // there is a header
        auto&& file_length = get_file_length(drug_file);
        std::cerr
            << std::endl
            << "============================================================"
            << std::endl;
        std::cerr << "Header line of prescription record: " << std::endl;
        std::getline(drug_file, line);
        std::cerr << line << std::endl;
        double prev_percentage = 0;
        std::vector<std::string> token;
        SQL gp_script("gp_script", db);
        gp_script.create_table(
            "CREATE TABLE gp_scripts("
            "ID INT NOT NULL, "
            "data_provider INT NOT NULL, "
            "date_Issue INT NOT NULL, "
            "Read2 Text Not Null, "
            "BNF_Code TEXT, "
            "DMD_Code TEXT, "
            "Drug_Name TEXT, "
            "Quantity TEXT, "
            "FOREIGN KEY (ID) REFERENCES Participant(ID),"
            "FOREIGN KEY (Data_Provider) REFERENCES gp_provider(ID));");
        gp_script.prep_statement(
            "INSERT INTO DATA_META(ID, data_provider, date_Issue, Read2, "
            "BNF_Code, DMD_Code, Drug_Name, Quantity) "
            "VALUES(@ID,@PROVIDER,@DATE,@READ2,@READ3,@VALUE1,"
            "@VALUE2,@VALUE3)");
        char* zErrMsg = nullptr;
        sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, &zErrMsg);
        while (std::getline(drug_file, line))
        {
            misc::trim(line);
            if (line.empty()) continue;
            print_progress(drug_file.tellg(), file_length, prev_percentage);
            // CSV input
            token = misc::csv_split(line);
            if (token.size() != 8)
            {
                throw std::runtime_error(
                    "Error: Undefined primary care record "
                    "format! File is expected to have exactly 8 columns.\n"
                    + line);
            }
            gp_script.run_statement(token);
        }
        drug_file.close();
        sqlite3_exec(db, "END TRANSACTION", nullptr, nullptr, &zErrMsg);
        fprintf(stderr, "\rProcessing %03.2f%%\n", 100.0);
        gp_script.create_index("drug_name_index",
                               std::vector<std::string> {"Drug_Name", "ID"});
        gp_script.create_index(
            "drug_name_date_index",
            std::vector<std::string> {"Drug_Name", "date_issue", "ID"});
        gp_script.create_index(
            "drug_name_provider_index",
            std::vector<std::string> {"Drug_Name", "data_provider", "ID"});
        gp_script.create_index(
            "drug_name_provider_date_index",
            std::vector<std::string> {"Drug_Name", "date_issue",
                                      "data_provider", "ID"});
    }
}

void usage()
{

    fprintf(stderr, " UK Biobank Phenotype Processing\n");
    fprintf(stderr, " Sam Choi\n");
    fprintf(stderr, " v0.2.1 ( 2019-10-18 )\n");
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
    fprintf(stderr, "    -g | --gp       gp_clinical table from ukbiobank\n");
    fprintf(stderr, "    -u | --drug     gp_scripts table from ukbiobank\n");
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
    static const char* optString = "d:c:p:o:m:g:u:rDh?";
    static const struct option longOpts[] = {
        {"data", required_argument, nullptr, 'd'},
        {"code", required_argument, nullptr, 'c'},
        {"pheno", required_argument, nullptr, 'p'},
        {"out", required_argument, nullptr, 'o'},
        {"memory", required_argument, nullptr, 'm'},
        {"gp", required_argument, nullptr, 'g'},
        {"drug", required_argument, nullptr, 'u'},
        {"replace", no_argument, nullptr, 'r'},
        {"danger", no_argument, nullptr, 'D'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0}};
    int longIndex = 0;
    int opt = 0;
    opt = getopt_long(argc, argv, optString, longOpts, &longIndex);
    std::string data_showcase, code_showcase, pheno_name, out_name,
        memory = "1024", gp_name, drug_name;
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
        case 'g': gp_name = optarg; break;
        case 'u': drug_name = optarg; break;
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

    std::unordered_set<std::string> included_fields;
    std::vector<std::string> pheno_names = misc::split(pheno_name, ",");
    char* zErrMsg = nullptr;
    sqlite3_exec(db, std::string("PRAGMA cache_size = " + memory).c_str(),
                 nullptr, nullptr, &zErrMsg);
    load_phenotype(db, included_fields, pheno_names, danger);
    load_data(db, included_fields, data_showcase);
    load_code(db, code_showcase);
    load_gp(db, gp_name, drug_name);
    sqlite3_close(db);
    return 0;
}
