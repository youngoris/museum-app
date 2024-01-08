const mysql = require('mysql');
const fs = require('fs');
const csv = require('fast-csv');
const path = require('path');

// CSV column names for museums.csv
const MUSEUM_COLUMNS = {
    ID: "ID",
    NAME: "Name",
    TYPE: "Type",
    STREET_ADDRESS: "Street Address",
    CITY: "City",
    STATE: "State",
    ZIP_CODE: "Zip Code",
    LATITUDE: "Latitude",
    LONGITUDE: "Longitude",
    EMPLOYER_ID_NUMBER: "Employer ID Number",
    TAX_PERIOD: "Tax Period",
    INCOME: "Income",
    REVENUE: "Revenue"
};

// CSV column names for gdp_by_states.csv
const GDP_COLUMNS = {
    STATE_ABBR: "State Abbr",
    STATE_NAME: "State Name",
    GDP: "GDP",
    PCE: "PCE",
    POPULATION: "Population"
};

// Database configuration
const dbConfig = {
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'MuseumDB'
};

// SQL table creation queries 
const createMuseumTable = `
CREATE TABLE IF NOT EXISTS Museum (
    ID BIGINT,
    Name VARCHAR(255) NOT NULL,
    TypeID INT,
    LocationID INT,
    FinancialID INT,
    FOREIGN KEY (TypeID) REFERENCES Type(TypeID),
    FOREIGN KEY (LocationID) REFERENCES Location(LocationID),
    FOREIGN KEY (FinancialID) REFERENCES Financial(FinancialID)
);
`;

const createTypeTable = `
CREATE TABLE IF NOT EXISTS Type (
    TypeID INT AUTO_INCREMENT PRIMARY KEY,
    TypeName VARCHAR(255) NOT NULL UNIQUE
);
`;

const createLocationTable = `
CREATE TABLE IF NOT EXISTS Location (
    LocationID INT AUTO_INCREMENT PRIMARY KEY,
    StreetAddress VARCHAR(255),
    CityID INT,
    StateID INT,
    ZipCode VARCHAR(10),
    Latitude DECIMAL(9,6),
    Longitude DECIMAL(9,6),
    FOREIGN KEY (CityID) REFERENCES City(CityID),
    FOREIGN KEY (StateID) REFERENCES State(StateID)
);
`;

const createFinancialTable = `
CREATE TABLE IF NOT EXISTS Financial (
    FinancialID INT AUTO_INCREMENT PRIMARY KEY,
    EmployerID VARCHAR(20),
    TaxPeriod VARCHAR(10),
    Income DECIMAL(15,2),
    Revenue DECIMAL(15,2)
);
`;

const createCityTable = `
CREATE TABLE IF NOT EXISTS City (
    CityID INT AUTO_INCREMENT PRIMARY KEY,
    CityName VARCHAR(255) NOT NULL UNIQUE
);
`;

const createStateTable = `
CREATE TABLE IF NOT EXISTS State (
    StateID INT AUTO_INCREMENT PRIMARY KEY,
    StateAbbre CHAR(2) NOT NULL UNIQUE,
    StateName VARCHAR(255) NOT NULL UNIQUE
);
`;

const createStatePopulationTable = `
CREATE TABLE IF NOT EXISTS StatePopulation (
    StateID INT,
    Population BIGINT,
    FOREIGN KEY (StateID) REFERENCES State(StateID)
);
`;

const createStateEconomicTable = `
CREATE TABLE IF NOT EXISTS StateEconomic (
    StateID INT,
    GDP BIGINT,
    PCE BIGINT,
    FOREIGN KEY (StateID) REFERENCES State(StateID)
);
`;


// Create a connection
const db = mysql.createConnection(dbConfig);


// Function to create tables
function createTables(callback) {
    console.log("Starting to create tables.");
    // Execute the queries to create tables in the correct order
    db.query(createTypeTable, (err) => {
        if (err) return callback(err);
        db.query(createCityTable, (err) => {
            if (err) return callback(err);
            db.query(createStateTable, (err) => {
                if (err) return callback(err);
                    db.query(createLocationTable, (err) => {
                        if (err) return callback(err);
                        db.query(createFinancialTable, (err) => {
                            if (err) return callback(err);
                                db.query(createStatePopulationTable, (err) => {
                                    if (err) return callback(err);
                                    db.query(createStateEconomicTable, (err) => {
                                        if (err) return callback(err);
                                        callback(null);
                                            db.query(createMuseumTable, (err) => {
                                                if (err) return callback(err);
                                });
                            });
                        });
                    });
                });
            });
            callback(null);
        });
    });
    console.log("Finished creating tables.");
}

// Function to load data from CSV
function loadDataFromCSV(filePath, processRow, callback) {
    console.log(`Loading data from ${filePath}`);
    let rows = [];
    fs.createReadStream(path.resolve(__dirname, filePath))
        .pipe(csv.parse({ headers: true, ignoreEmpty: true }))
        .on('data', row => rows.push(row))
        .on('end', () => {
            console.log(`Finished loading ${rows.length} rows from ${filePath}`);
            processRowsSequentially(rows, 0, processRow, callback);
        })
        .on('error', error => {
            console.error(`Error loading data from ${filePath}: ${error}`);
            callback(error);
        });
}

    
function processRowsSequentially(rows, index, processRow, callback) {
    if (index >= rows.length) {
        console.log("All rows processed");
        return callback(null);
    }
    console.log(`Processing row ${index + 1}`);
    processRow(rows[index], (err) => {
        if (err) {
            console.error(`Error processing row ${index + 1}: ${err}`);
            return callback(err);
        }
        processRowsSequentially(rows, index + 1, processRow, callback);
    });
}




function processMuseumRow(row, callback) {
    insertUniqueValue('Type', 'TypeName', row[MUSEUM_COLUMNS.TYPE], null, (err, typeID) => {
        if (err) return callback(err);

        insertUniqueValue('City', 'CityName', row[MUSEUM_COLUMNS.CITY], null, (err, cityID) => {
            if (err) return callback(err);

            // 注意这里应该传递 State 名称作为额外值
            insertUniqueValue('State', 'StateAbbre', row[MUSEUM_COLUMNS.STATE], "State Name Here", (err, stateID) => {
                if (err) return callback(err);
                const locationData = [row[MUSEUM_COLUMNS.STREET_ADDRESS], cityID, stateID, row[MUSEUM_COLUMNS.ZIP_CODE], row[MUSEUM_COLUMNS.LATITUDE], row[MUSEUM_COLUMNS.LONGITUDE]];
                db.query('INSERT INTO Location (StreetAddress, CityID, StateID, ZipCode, Latitude, Longitude) VALUES (?, ?, ?, ?, ?, ?)', locationData, (err, result) => {
                    if (err) {
                        console.error("Error inserting location: ", err);
                        return callback(err);
                    }
                    const locationID = result.insertId;
                    // Handle empty values for TaxPeriod, Income, and Revenue
                    const taxPeriod = row[MUSEUM_COLUMNS.TAX_PERIOD].trim() ? row[MUSEUM_COLUMNS.TAX_PERIOD].trim() : null;
                    const income = row[MUSEUM_COLUMNS.INCOME].trim() ? row[MUSEUM_COLUMNS.INCOME].trim() : null;
                    const revenue = row[MUSEUM_COLUMNS.REVENUE].trim() ? row[MUSEUM_COLUMNS.REVENUE].trim() : null;

                    // Financial data insertion
                    const financialData = [row[MUSEUM_COLUMNS.EMPLOYER_ID_NUMBER], taxPeriod, income, revenue];
                    db.query('INSERT INTO Financial (EmployerID, TaxPeriod, Income, Revenue) VALUES (?, ?, ?, ?)', financialData, (err, result) => {
                        if (err) throw err;
                        const financialID = result.insertId;

                        // 最后插入 Museum
                        const museumData = [row['ID'], row['Name'], typeID, locationID, financialID];
                        db.query('INSERT INTO Museum (ID, Name, TypeID, LocationID, FinancialID) VALUES (?, ?, ?, ?, ?)', museumData, (err, result) => {
                            if (err) throw err;

                            console.log("Museum row inserted");
                    
                            // Call the callback if it's a function
                            if (typeof callback === 'function') {
                                callback();
                            }
                        });
                    });
                });
            });
        });
    });
}

// Inserting unique values into Type, City, and State tables
function insertUniqueValue(table, column, value, additionalValue, callback) {
        db.query(`SELECT * FROM ${table} WHERE ${column} = ?`, [value], (err, results) => {
            if (err) {
                console.error(`Error querying ${table}:`, err);
                return callback(err);
            }

            if (results.length > 0) {
                const existingID = results[0][`${table}ID`]; 
                console.log(`${table} already exists: ${existingID}`);
                return callback(null, existingID);
            }

            let insertQuery = `INSERT INTO ${table} (${column}) VALUES (?)`;
            let queryParams = [value];

            if (table === 'State') {
                insertQuery = `INSERT INTO State (StateAbbre, StateName) VALUES (?, ?)`;
                queryParams = [value, getStateFullName(value)]; 
            }

            db.query(insertQuery, queryParams, (err, result) => {
                if (err) {
                    console.error(`Error inserting into ${table}:`, err);
                    return callback(err);
                }
                console.log(`Inserted into ${table}: ${result.insertId}`);
                callback(null, result.insertId); 
            });
        });
    }





    function getStateFullName(stateAbbre) {
        // 从州缩写获取州全名的函数
        const stateAbbreToFullName = {
            'AL': 'Alabama',
            'AK': 'Alaska',
            'AZ': 'Arizona',
            'AR': 'Arkansas',
            'CA': 'California',
            'CO': 'Colorado',
            'CT': 'Connecticut',
            'DE': 'Delaware',
            'FL': 'Florida',
            'GA': 'Georgia',
            'HI': 'Hawaii',
            'ID': 'Idaho',
            'IL': 'Illinois',
            'IN': 'Indiana',
            'IA': 'Iowa',
            'KS': 'Kansas',
            'KY': 'Kentucky',
            'LA': 'Louisiana',
            'ME': 'Maine',
            'MD': 'Maryland',
            'MA': 'Massachusetts',
            'MI': 'Michigan',
            'MN': 'Minnesota',
            'MS': 'Mississippi',
            'MO': 'Missouri',
            'MT': 'Montana',
            'NE': 'Nebraska',
            'NV': 'Nevada',
            'NH': 'New Hampshire',
            'NJ': 'New Jersey',
            'NM': 'New Mexico',
            'NY': 'New York',
            'NC': 'North Carolina',
            'ND': 'North Dakota',
            'OH': 'Ohio',
            'OK': 'Oklahoma',
            'OR': 'Oregon',
            'PA': 'Pennsylvania',
            'RI': 'Rhode Island',
            'SC': 'South Carolina',
            'SD': 'South Dakota',
            'TN': 'Tennessee',
            'TX': 'Texas',
            'UT': 'Utah',
            'VT': 'Vermont',
            'VA': 'Virginia',
            'WA': 'Washington',
            'WV': 'West Virginia',
            'WI': 'Wisconsin',
            'WY': 'Wyoming'
        };

        return stateAbbreToFullName[stateAbbre];
    }


//     if (table === 'State') {
//         db.query(`SELECT * FROM State WHERE StateAbbre = ?`, [value], (err, results) => {
//             if (err) {
//                 return callback(err);
//             }
//             if (results.length === 0) {
//                 // 确保 additionalValue 是有效的州全名
//                 const stateFullName = getStateFullName(value); // 从州缩写获取州全名的函数
//                 db.query(`INSERT INTO State (StateAbbre, StateName) VALUES (?, ?)`, [value, stateFullName], handleInsert);
//             } else {
//                 callback(null, results[0].StateID);
//             }
//         });
//     } else {
//         db.query(`SELECT * FROM ${table} WHERE ${column} = ?`, [value], (err, results) => {
//             if (err) {
//                 console.error(`Error querying ${table}:`, err);
//                 callback(err);
//             } else if (results.length === 0) {
//                 db.query(`INSERT INTO ${table} (${column}) VALUES (?)`, [value], handleInsert);
//             } else {
//                 callback(null, results[0][`${table}ID`]);
//             }
//         });
//     }
// }








function processGDPRow(row, callback) {
    // Fetch StateID using StateAbbre from the State table
    db.query('SELECT StateID FROM State WHERE StateAbbre = ?', [row[GDP_COLUMNS.STATE_ABBR]], (err, results) => {
        if (err) {
            console.error('Error querying StateID:', err);
            return callback(err);
        }

        // Handle case where State is not found
        if (results.length === 0) {
            console.error('No matching StateID found for State Abbr:', row[GDP_COLUMNS.STATE_ABBR]);
            return callback(new Error('No matching StateID found for State Abbr: ' + row[GDP_COLUMNS.STATE_ABBR]));
        }

        const stateID = results[0].StateID;

        // Insert or update Population data
        const populationQuery = 'REPLACE INTO StatePopulation (StateID, Population) VALUES (?, ?)';
        db.query(populationQuery, [stateID, row[GDP_COLUMNS.POPULATION]], (popErr) => {
            if (popErr) {
                console.error('Error inserting/updating Population data:', popErr);
                return callback(popErr);
            }

            console.log('Population data inserted/updated successfully for StateID:', stateID);

            // Insert or update Economic data
            const economicQuery = 'REPLACE INTO StateEconomic (StateID, GDP, PCE) VALUES (?, ?, ?)';
            db.query(economicQuery, [stateID, row[GDP_COLUMNS.GDP], row[GDP_COLUMNS.PCE]], (ecoErr) => {
                if (ecoErr) {
                    console.error('Error inserting/updating Economic data:', ecoErr);
                    return callback(ecoErr);
                }

                console.log('Economic data inserted/updated successfully for StateID:', stateID);
                callback(null); // Successfully processed the row
            });
        });
    });
}




// Main execution flow
console.log("Starting database table creation.");

createTables((err) => {
    if (err) {
        console.error('Error creating tables: ', err);
        db.end();
        return;
    }

    console.log("Tables created successfully.");

    console.log("Loading data from museums.csv");
    loadDataFromCSV('./data/museums.csv', processMuseumRow, (err) => {
        if (err) {
            console.error('Error loading museums data: ', err);
            db.end();
            return;
        }

        console.log("Museum data loaded successfully.");

        console.log("Loading data from gdp_by_states.csv");
        loadDataFromCSV('./data/gdp_by_states.csv', processGDPRow, (err) => {
            if (err) {
                console.error('Error loading GDP data: ', err);
                db.end();
                return;
            }

            console.log("GDP data loaded successfully.");
            db.end();
        });
    });
});


