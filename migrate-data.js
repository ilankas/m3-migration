/*
mkdir m3_migration
cd m3_migration
mkdir data

npm init -y
npm i async mongodb

mongod --dbpath C:\nodejs\learning\m3_migration\data\

*/
const url = 'mongodb://localhost:27017/m3-customers'

const fs   = require('fs')
const path = require('path')
const mongodb = require('mongodb')
const async = require('async')

const custData = require(path.join(__dirname, 'files', 'm3-customer-data.json'));
const addrData = require(path.join(__dirname, 'files', 'm3-customer-address-data.json'));

var recordsPerBatch = Number(process.argv[2]);
if (!recordsPerBatch) recordsPerBatch = 500;
var asyncTasks = [];


mongodb.MongoClient.connect(url, (error, client) => {
  if (error) return process.exit(1)
  var db = client.db('m3-customers-db')

  // append customers data with address data
  custData.forEach((customer, i, list) => {
	custData[i] = Object.assign(customer, addrData[i]);
  });

  // single asynchronous function declaration
  const asyncTask = function(numThread) {
	  return (callback) => {
		  var startIdx = numThread * recordsPerBatch;
		  var endIdx = (numThread + 1) * recordsPerBatch;
		  
		  console.log('Inserting records from: ' + startIdx + ' to: ' + endIdx)
		  db.collection('customers').insert(
			custData.slice(startIdx, endIdx), (error, results) => {
				callback(error, results);
			}
		  )
	  }
	  
  };
  
  // fill array with asynchronous functions
  for (var i = 0; i < Math.ceil(custData.length / recordsPerBatch); i++) {
	  console.log('Declared function: ' + (i + 1));
	  asyncTasks.push(asyncTask(i));
  };
  
  // Log start time
  const startTime = Date.now();
  console.log('Processing ' + asyncTasks.length + ' task' + ((asyncTasks.length > 1) ? 's' : ''));
  
  async.parallel(asyncTasks, (error, results) => {
    if (error) console.log('Error: ', error)
  	const endTime = Date.now();
	console.log(`Done in ${endTime - startTime} milliseconds.`)
    client.close();
  })  
});

