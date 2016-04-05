
MongoLS

=========

A JavaScript implementation of the mongo query api for plain objects and HTML5 localStorage.

[![Build Status](https://travis-ci.org/belteshazzar/MongoLS.svg?branch=master)](https://travis-ci.org/belteshazzar/MongoLS)

[![Coverage Status](https://coveralls.io/repos/github/belteshazzar/MongoLS/badge.svg?branch=master)](https://coveralls.io/github/belteshazzar/MongoLS?branch=master)

## Installation

  `npm install mongols`

## Usage

    var mongols = require('mongols');
    var db = new mongo.DB()
    var sample = db.createCollection("sample")
    sample.insert({ age: 4,	legs: 0	});
    sample.insert([{ age: 4,	legs: 5	},{ age: 54, legs: 2	}]);
    sample.insertMany([{ age: 54, legs: 12 },{ age: 16					 }]);
    sample.insertOne({ name: "steve"		 });
    sample.find();
    sample.find({ $and: [{ age : 54},{ legs: 2 }] })

## Tests

  `npm test`

## Contributing

If you find this useful ... well, lets not kid around, it basically works, but there is a lot of things to fix up. All help is greatly appreciated!
