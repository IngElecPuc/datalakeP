#Querys for RedShift -> redshift_loader.py

# Query available tables, excluding internal schemes
QUERY_TABLE_NAMES = """
SELECT schemaname, tablename
FROM pg_catalog.pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
"""

CREATE_GENDER_SUBMISSION = """
CREATE TABLE gender_submission(
    id INTEGER IDENTITY(1,1),
    PassengerId INTEGER NOT NULL,
    Survived BOOLEAN NOT NULL
) DISTSTYLE AUTO;
"""

CREATE_TRAIN_DATA = """
CREATE TABLE train_data(
    id INTEGER IDENTITY(1,1),
    PassengerId INTEGER NOT NULL,
    Survived BOOLEAN NOT NULL,
    Pclass INTEGER NOT NULL,
    "Name" VARCHAR(100) NOT NULL,
    Sex VARCHAR(6) NOT NULL,
    "Age" DECIMAL(4,2) NULL,
    SibSp INTEGER NOT NULL,
    Parch INTEGER NOT NULL,
    Ticket VARCHAR(20) NOT NULL,
    Fare REAL NOT NULL,
    Cabin VARCHAR(15) NULL,
    Embarked CHAR(1) NULL
) DISTSTYLE AUTO;
"""

CREATE_TEST_DATA = """
CREATE TABLE test_data(
    id INTEGER IDENTITY(1,1),
    PassengerId INTEGER NOT NULL,
    Pclass INTEGER NOT NULL,
    "Name" VARCHAR(100) NOT NULL,
    Sex VARCHAR(6) NOT NULL,
    "Age" DECIMAL(4,2) NULL,
    SibSp INTEGER NOT NULL,
    Parch INTEGER NOT NULL,
    Ticket VARCHAR(20) NOT NULL,
    Fare REAL NULL,
    Cabin VARCHAR(15) NULL,
    Embarked CHAR(1) NULL
) DISTSTYLE AUTO;
"""

QUERY_COL_NAMES = """
SELECT column_name
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
AND table_name = 'X'
ORDER BY table_schema, table_name, ordinal_position;
"""