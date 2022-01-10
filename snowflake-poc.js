// var snowflake = require('snowflake-sdk');

// var connection = snowflake.createConnection({
//   account: 'ca11441.east-us-2',
//   username: 'PrajactSawant',
//   password: 'Srijan@123'
// });

// connection.connect(function(err, conn) {
//   if (err) {
//     console.error('Unable to connect: ' + err.message);
//   } else {
//     console.log('conn: ', conn);
//     console.log('Successfully connected as id: ' + connection.getId());
//   }
// });


const snowflake = require('snowflake-sdk');
snowflake.configure({ ocspFailOpen: false });

const snowflakeDatabase = {
  account: 'nu27892.ap-south-1',
  username: 'bibekjoshisrijan1',
  password: 'bibekjoshisrijan1@Srijan@123'
};

var connection = snowflake.createConnection(snowflakeDatabase);
console.log('connection: ', connection.getId())

createPool = () => {
  console.log('connection :', connection);
    if (!connection) {
        connection = snowflake.createConnection(snowflakeDatabase);
    }

    if (!connection.isUp()) {
        connection.connect(function (err, conn) {
            if (err) {
                console.error('Unable to connect: ' + err.message);
            } else {
                console.log('Successfully connected to Snowflake.');
                // Optional: store the connection ID.
                connection_ID = conn.getId();
            }
        });
        console.log('connection status:', connection.isUp());
    }

    return connection;
};

closePool = () => {
    connection.destroy(function (err, conn) {
        if (err) {
            console.error('Unable to disconnect: ' + err.message);
        } else {
            console.log(
                'Disconnected connection with id: ' + connection.getId()
            );
        }
    });
};

module.exports = {
    createPool: createPool,
    closePool: closePool,
};
