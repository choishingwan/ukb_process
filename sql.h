#ifndef PROCESS_SQL_H
#define PROCESS_SQL_H

#include <iostream>
#include <sqlite3.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <assert.h> 
class SQL
{
public:
    SQL(const std::string& name, sqlite3* dat);
    void create_table(const std::string& sql);
    void prep_statement(const std::string& sql);
    void run_statement(const std::vector<std::string>& token,
                       const size_t range, const size_t begin = 0)
    {
        bind_statement(token, range, begin);
        process_statement();
    }

    void run_statement(const std::vector<std::string>& token,
                       const size_t begin = 0)
    {
        bind_statement(token, token.size(), begin);
        process_statement();
    }
    void create_index(const std::string& index_name,
                      const std::vector<std::string>& fields);
    void execute_sql(const std::string& sql)
    {
        if (!m_table_created)
        {
            throw std::runtime_error("Error: Table: " + m_table_name
                                     + "not created");
        }
        char* zErrMsg = nullptr;
        int rc = sqlite3_exec(m_db, sql.c_str(), callback, nullptr, &zErrMsg);
        if (rc != SQLITE_OK)
        {
            std::string error = zErrMsg;
            sqlite3_free(zErrMsg);
            throw std::runtime_error("SQL error: " + error);
        }
    }

private:
    sqlite3* m_db;
    sqlite3_stmt* m_statement;
    std::string m_table_name;
    bool m_table_created = false;
    static int callback(void* /*NotUsed*/, int argc, char** argv,
                        char** azColName)
    {
        int i;
        for (i = 0; i < argc; i++)
        { printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL"); }
        printf("\n");
        return 0;
    }


    void process_statement()
    {
        int status = sqlite3_step(m_statement);
        if (status != SQLITE_DONE || status == SQLITE_ERROR
            || status == SQLITE_BUSY)
        {
            throw std::runtime_error("Error: Insert failed: "
                                     + std::string(sqlite3_errmsg(m_db)) + " ("
                                     + std::to_string(status) + ")");
        }
        sqlite3_clear_bindings(m_statement);
        sqlite3_reset(m_statement);
    }

    void bind_statement(const std::vector<std::string>& token,
                        const size_t range, const size_t begin = 0)
    {
        assert(range <= token.size());
        assert(begin < range);
        for (size_t i = begin; i < range; ++i)
        {
            sqlite3_bind_text(m_statement, static_cast<int>(i + 1),
                              token[i].c_str(), -1, SQLITE_TRANSIENT);
        }
    }
    void bind_statement(const std::vector<std::string>& token,
                        const size_t begin = 0)
    {
        bind_statement(token, token.size(), begin);
    }
};

#endif // PROCESS_SQL_H
