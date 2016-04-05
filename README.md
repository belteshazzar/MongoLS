
# MongoLS

A JavaScript implementation of the mongo query api for plain objects and HTML5 localStorage.

There are two main use cases that MongoLS targets:
- providing a mongo interface to localStorage in HTML5 web browsers
- for use as an in memory mongo database that can be used in the browser or nodejs

[![Build Status](https://travis-ci.org/belteshazzar/MongoLS.svg?branch=master)](https://travis-ci.org/belteshazzar/MongoLS) [![Coverage Status](https://coveralls.io/repos/github/belteshazzar/MongoLS/badge.svg?branch=master)](https://coveralls.io/github/belteshazzar/MongoLS?branch=master)

## In Node.js

### Installation

  `npm install mongols`

### Usage

    var mongols = require('mongols');
    var db = new mongo.DB()
    db.createCollection("sample")
    db.sample.insert({ age: 4,	legs: 0	});
    db.sample.insert([{ age: 4,	legs: 5	},{ age: 54, legs: 2	}]);
    db.sample.insertMany([{ age: 54, legs: 12 },{ age: 16					 }]);
    db.sample.insertOne({ name: "steve"		 });
    var cur = db.sample.find({ $and: [{ age : 54},{ legs: 2 }] })
    cur.next()

### Tests

  `npm test`

## In the Browser

For an example of MongoLS in the browser check out: http://belteshazzar.github.io/MongoLS/index.html

### Usage

Download from here: https://raw.githubusercontent.com/belteshazzar/MongoLS/master/mongols.js

Include in your web page:

    <script src="mongols.js"></script>
    
Query localStorage or other collections (note that in the browser the localStorage collection is automatically created and because it is backed by HTML5 localStorage it will persist across multiple sessions):

    <script>
       var db = new MongoLS.DB()
       db.localStorage.insert({ age: 4,	legs: 0	});
       var cur = db.localStorage.find();
       alert(JSON.stringify(cur.toArray()));
    </script>

## Contributing

If you find this useful ... well, lets not kid around, it basically works, but there is a lot of things to fix up. All help is greatly appreciated!
