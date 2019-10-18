#include "sql.h"

SQL::SQL(const std::string& name, sqlite3* dat) : m_db(dat), m_table_name(name)
{
}
void SQL::create_table(const std::string& sql)
{
    try
    {
        execute_sql(sql);
        std::cerr << "Table: " << m_table_name << " created sucessfully\n";
        m_table_created = true;
    }
    catch (const std::runtime_error& er)
    {
        throw std::runtime_error(er.what());
    }
}

void SQL::prep_statement(const std::string& sql)
{
    if (!m_table_created)
    {
        throw std::runtime_error("Error: Table: " + m_table_name
                                 + "not created");
    }
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &m_statement, nullptr);
}


void SQL::create_index(const std::string& index_name,
                       const std::vector<std::string>& fields)
{
    std::string sql =
        "CREATE INDEX '" + index_name + "' ON '" + m_table_name + "' (";
    bool has_comma = false;
    for (auto&& f : fields)
    {
        std::string comma = (has_comma ? "," : "");
        sql += comma + "'" + f + "'";
        has_comma = true;
    }
    sql += ")";
    execute_sql(sql);
}
