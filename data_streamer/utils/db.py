import logging
import os
from datetime import datetime

from mysql.connector.aio import connect

mariadb_host = os.environ.get("MARIADB_HOST")
mariadb_user = os.environ.get("MARIADB_USER")
mariadb_password = os.environ.get("MARIADB_PASSWORD")
mariadb_database = os.environ.get("MARIADB_DATABASE")
connection_data = {
    "host": mariadb_host,
    "user": mariadb_user,
    "password": mariadb_password,
    "database": mariadb_database,
    "autocommit": True,
}

sources_table_name = os.environ.get("SOURCES_TABLE_NAME")
news_table_name = os.environ.get("NEWS_TABLE_NAME")

logger = logging.getLogger(__name__)


async def _execute_command(command: str, data=None):
    logger.info(f"Executing command: {command}: start")
    try:
        async with await connect(**connection_data) as cnx:
            async with await cnx.cursor(dictionary=True) as cursor:
                await cursor.execute(command, data)
                if cursor.with_rows:
                    rows = await cursor.fetchall()
                    logger.info(f"Command: {command}; Output: {rows}")
                    return rows
    except Exception:
        logger.exception(f"Failed to execute command: {command}")
        raise
    finally:
        logger.info(f"Executing command: {command}: exit")


async def init_db(sources_list: list[str] = None):
    await _execute_command(
        f"CREATE TABLE IF NOT EXISTS `{news_table_name}` ("
        "id INT AUTO_INCREMENT PRIMARY KEY, "
        "news_key VARCHAR(1024) NOT NULL UNIQUE, "
        "title VARCHAR(512) NOT NULL, "
        "summary VARCHAR(1024) NOT NULL, "
        "link VARCHAR(1024) NOT NULL UNIQUE, "
        "timedate DATETIME DEFAULT CURRENT_TIMESTAMP, "
        "text VARCHAR(4096), "
        "language VARCHAR(64)"
        ");"
    )

    await _execute_command(
        f"CREATE TABLE IF NOT EXISTS `{sources_table_name}` ("
        "id INT AUTO_INCREMENT PRIMARY KEY, "
        "link VARCHAR(1024) NOT NULL UNIQUE, "
        "is_enabled BOOL DEFAULT TRUE"
        ");"
    )

    is_sources_non_empty = await _execute_command(f"SELECT 1 FROM `{sources_table_name}` LIMIT 1;")

    if sources_list and not is_sources_non_empty:
        placeholders = ", ".join(["(%s)"] * len(sources_list))
        values = [source for source in sources_list]  # плоский список
        await _execute_command(f"INSERT INTO `{sources_table_name}` (link) VALUES {placeholders}", values)


async def get_all_sources() -> list[str]:
    select_command = f"SELECT (link) FROM `{sources_table_name}` WHERE is_enabled=TRUE"
    sources = await _execute_command(select_command)
    return [source_record.get("link") for source_record in sources]


async def add_news_item(
    key: str, title: str, summary: str, link: str, timedate: datetime = None, text: str = None, language: str = None
):
    columns = ["key", "title", "summary", "link"]
    values = [key, title, summary, link]
    if timedate is not None:
        columns.append("timedate")
        values.append(timedate)
    if text is not None:
        columns.append("text")
        values.append(text)
    if language is not None:
        columns.append("language")
        values.append(language)

    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(values))
    insert_command = f"INSERT INTO `{news_table_name}` ({columns_str}) VALUES ({placeholders})"
    await _execute_command(insert_command, values)


async def get_news_item_by_key(key: str) -> dict:
    select_command = f"SELECT * FROM `{news_table_name}` WHERE news_key=%s"
    news_items = await _execute_command(select_command, data=(key,))
    if news_items:
        return news_items[0]
    return None
