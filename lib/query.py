PSQL_CREATE_ACCOUNTS_TABLE = """CREATE TABLE accounts (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    age SMALLINT,
                    birth_date DATE,
                    account_balance NUMERIC(10, 2),
                    is_active BOOLEAN,
                    signup_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP,
                    bio TEXT,
                    profile_picture BYTEA,
                    favorite_color TEXT CHECK (favorite_color IN ('red', 'green', 'blue')),
                    height REAL,
                    weight DOUBLE PRECISION
                );"""
PSQL_INSERT_ACCOUNTS_SAMPLE_DATA = """INSERT INTO accounts
(name, age, birth_date, account_balance, is_active, signup_time, last_login, bio, profile_picture, favorite_color, height, weight)
VALUES
('Alice', 30, '1991-05-21', 1500.00, TRUE, '2021-01-08 09:00:00', '2021-03-10 08:00:00', 'Bio of Alice', NULL, 'red', 1.70, 60.5);"""

PSQL_CREATE_AUTHORS_TABLE = """CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    nationality VARCHAR(50),
    biography TEXT,
    email VARCHAR(255),
    phone_number VARCHAR(20),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""

PSQL_INSERT_AUTHORS_SAMPLE_DATA =  """INSERT INTO authors (first_name, last_name, date_of_birth, nationality, biography, email, phone_number)
VALUES
('John', 'Doe', '1980-01-01', 'American', 'Biography of John Doe.', 'john.doe@example.com', '123-456-7890');"""

PSQL_CREATE_BOOKS_TABLE = """CREATE TABLE books (
    book_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    author_id INT,
    publish_date DATE,
    isbn VARCHAR(20),
    genre VARCHAR(100),
    page_count INT,
    publisher VARCHAR(100),
    language VARCHAR(50),
    available_copies INT,
    total_copies INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);
"""
PSQL_INSERT_BOOKS_SAMPLE_DATA = """INSERT INTO books (title, author_id, publish_date, isbn, genre, page_count, publisher, language, available_copies, total_copies)
VALUES
('The Great Adventure', 1, '2020-06-01', '978-3-16-148410-0', 'Adventure', 300, 'Adventure Press', 'English', 10, 20);"""

ALTER_TABLES = [
    # control: column-type-change -> books
    "ALTER TABLE books ALTER COLUMN isbn TYPE VARCHAR(30);",
    
    # control: drop-column -> accounts
    "ALTER TABLE accounts DROP COLUMN profile_picture;",

    # control: add-column with default value -> authors
    "ALTER TABLE authors ADD COLUMN is_available BOOLEAN DEFAULT TRUE;",
]

CREATE_TABLES = [
    PSQL_CREATE_AUTHORS_TABLE,
    PSQL_CREATE_ACCOUNTS_TABLE,
    PSQL_CREATE_BOOKS_TABLE,
]

DROP_TABLES = [
    "DROP TABLE IF EXISTS books;",
    "DROP TABLE IF EXISTS accounts;",
    "DROP TABLE IF EXISTS authors;",
]

PRESEED_DATA = [
    PSQL_INSERT_AUTHORS_SAMPLE_DATA,
    PSQL_INSERT_ACCOUNTS_SAMPLE_DATA,
    PSQL_INSERT_BOOKS_SAMPLE_DATA,
]
