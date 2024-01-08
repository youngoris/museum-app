const mysql = require('mysql');

// Database configuration
const dbConfig = {
    host: 'localhost',
    user: 'root',
    password: 'password'
};

// Create a connection
const db = mysql.createConnection(dbConfig);

// Connect to MySQL
db.connect(err => {
    if (err) {
        console.error('Error connecting: ' + err.stack);
        return;
    }
    console.log('Connected as ID ' + db.threadId);

    // Check if the database exists
    const checkDatabaseQuery = 'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = "MuseumDB"';
    db.query(checkDatabaseQuery, (err, result) => {
        if (err) {
            console.error('Error checking database: ' + err.stack);
            db.end();
            return;
        }

        if (result.length > 0) {
            console.log('Database MuseumDB already exists');
            db.end();
        } else {
            // Create the database if it doesn't exist
            const createDatabaseQuery = 'CREATE DATABASE IF NOT EXISTS MuseumDB';
            db.query(createDatabaseQuery, (err, result) => {
                if (err) {
                    console.error('Error creating database: ' + err.stack);
                    db.end();
                    return;
                }
                console.log('Database MuseumDB created');
                db.end();
            });
        }
    });
});
