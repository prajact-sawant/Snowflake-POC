var snowflake = require('snowflake-sdk');
// bibek's acc-creds
var connection = snowflake.createConnection({
  account: 'nu27892.ap-south-1',
  username: 'bibekjoshisrijan1',
  password: 'bibekjoshisrijan1@Srijan@123'
});

connection.connect(function(err, conn) {
  if (err) {
    console.error('Unable to connect: ' + err.message);
  } else {
    console.log('Successfully connected as id: ' + connection.getId());

    // var statement = connection.execute({
    //   sqlText: 'create database poc_testdb',
    //   complete: function(err, stmt, rows) {
    //     if (err) {
    //       console.error('Failed to execute statement due to the following error: ' + err.message);
    //     } else {
    //       console.log('Successfully executed statement: ' + stmt.getSqlText());
    //     }
    //   }
    // });


    connection.execute({
      sqlText: 'use database poc_testdb;',
      complete: function(err, stmt, rows) {
        if (err) {
          console.error('Failed to execute statement due to the following error: ' + err.message);
        } else {
          console.log('Successfully => USE DATABASE poc_testdb');

          // connection.execute({
          //   sqlText: "create table myjsontable (json_data variant);",
          //   complete: function(err, stmt, rows) {
          //     if (err) {
          //       console.error('Failed to create =>\t' + err);
          //     } else {
          //       console.log('craeteJsonTable ==> Successfully => create or replace temporary table ' + stmt, rows);
          //     }
          //   }
          // });

          // create warehouse
          // const craeteWarehouse = connection.execute({
          //   sqlText: "create warehouse mywarehouse with warehouse_size='X-SMALL' auto_suspend = 120 auto_resume = true initially_suspended=true;",
          //   complete: function(err, stmt, rows) {
          //     if (err) {
          //       console.error('Failed to create =>\t' + err);
          //     } else {
          //       console.log('Successfully => create or replace temporary table ' + stmt, rows);
          //     }
          //   }
          // });
          // console.log('craeteTable: ', craeteWarehouse, '\n===========================\n\n\n');


          // create warehouse
          // connection.execute({
          //   sqlText: "alter warehouse mywarehouse resume;",
          //   complete: function(err, stmt, rows) {
          //     if (err) {
          //       console.error('Failed to create =>\t' + err);
          //     } else {
          //       console.log('restoreWarehouse ==> Successfully => create or replace temporary table ' + stmt, rows);
          //     }
          //   }
          // });

          connection.execute({
            sqlText: 'put file://./sales.json @my_json_stage auto_compress=true;',
            complete: function(err, stmt, rows) {
              if (err) {
                console.error('Failed to put =>\t: ' + err.message);
              } else {
                console.log('stageFiles ==> ' + stmt, rows);

                // list all staged data
                connection.execute({
                  sqlText: 'list @my_json_stage;',
                  complete: function(err, stmt, rows) {
                    if (err) {
                      console.error('Failed to put =>\t: ' + err.message);
                    } else {
                      console.log('listStageFiles ==> ' + stmt, JSON.stringify(rows));

                      // copy from staged data to tables
                      connection.execute({
                        sqlText: 'copy into myjsontable from @my_json_stage/sales.json.gz file_format = (format_name = myjsonformat) on_error = "skip_file";',
                        complete: function(err, stmt, rows) {
                          if (err) {
                            console.error('Failed to put =>\t: ' + err.message);
                          } else {
                            console.log('copyDataIntoTarget ==> ' + stmt, rows);

                            // select the json-table
                            connection.execute({
                              sqlText: 'select * from myjsontable;',
                              complete: function(err, stmt, rows) {
                                if (err) {
                                  console.error('Failed to put =>\t: ' + err.message);
                                } else {
                                  console.log('getJSONData ==> ' + stmt, JSON.stringify(rows));

                                  // remove all staged-data from Stage, once copy is done to table
                                  // connection.execute({
                                  //   sqlText: "remove @my_json_stage pattern='.*.json.gz';",
                                  //   complete: function(err, stmt, rows) {
                                  //     if (err) {
                                  //       console.error('Failed to put =>\t: ' + err.message);
                                  //     } else {
                                  //       console.log('removeStagedFileOnSuccess ==> ' + stmt, rows);
                                  //     }
                                  //   }
                                  // });
                                }
                              }
                            });
                          }
                        }
                      });
                    }
                  }
                });
              }
            }
          });
        }
      }
    });



    // const createFileFormat = connection.execute({
    //   sqlText: 'create file format myjsonformat type = "JSON" strip_outer_array = true;',
    //   complete: function(err, stmt, rows) {
    //     if (err) {
    //       console.error('Failed to execute statement due to the following error: ' + err.message);
    //     } else {
    //       console.log('Successfully => create or replace file format\t' + stmt, rows);
    //     }
    //   }
    // });
    // console.log('createFileFormat: ', createFileFormat, '\n===========================\n\n\n');

    // const createStage = connection.execute({
    //   sqlText: 'create stage my_json_stage file_format = myjsonformat',
    //   complete: function(err, stmt, rows) {
    //     if (err) {
    //       console.error('Failed to execute statement due to the following error: ' + err.message);
    //     } else {
    //       console.log('Successfully => create or replace file format\t' + stmt, rows);
    //     }
    //   }
    // });
    // console.log('createStage: ', createStage, '\n===========================\n\n\n');






  }
});

