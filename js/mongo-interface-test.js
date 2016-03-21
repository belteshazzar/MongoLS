function testDatabase(db) {

  function testCreateCollection() {
    if (db.getCollectionNames().length==0) throw "no databases to drop";
    db.dropDatabase();
    if (db.getCollectionNames().length!=0) throw "shouldn't be any collections";
    db.createCollection("myCollection");
    db.createCollection("localStorage");
    if (db.getCollectionNames().length!=2) throw "failed to create my collection";
    if (!db.myCollection) throw "myCollection doesn't seem to exist?";
    if (!db.localStorage) throw "localStorage doesn't seem to exist?";
  }

  function testDropDatabase() {
    // covered by testCreateCollection
  }

  function testGetCollectionNames() {
    // covered by testCreateCollection
  }

  testCreateCollection();
  testDropDatabase();
  testGetCollectionNames();
}


function testCollection(db,collectionName,log) {


  function testFind(q) { 
    log("--- test : " + JSON.stringify(q) + " ---");
    try {
      var results = [];
      var docs = db[collectionName].find(q);//.sort({ age: 1, legs: -1 });
      while (docs.hasNext()) {
        results.push(docs.next());
      }

      for (var i=0 ; i<results.length ; i++) {
        log(JSON.stringify(results[i]));
      }
      return results;
    } catch (e) {
        log(JSON.stringify(e));
      return e;
    }
  }

  function dump() {
    var c = db[collectionName].find();
    while (c.hasNext()) {
      console.log(c.next());
    }
  }

  function reset() {
    db.dropDatabase();
    db.createCollection(collectionName);
    db[collectionName].insert({ age: 4,  legs: 0  });
    db[collectionName].insert([{ age: 4,  legs: 5  },{ age: 54, legs: 2  }]);
    db[collectionName].insertMany([{ age: 54, legs: 12 },{ age: 16           }]);
    db[collectionName].insertOne({ name: "steve"     });
  }

  function testCount() {
    reset();
    var q = { age : { $gt:3, $lt:7 }};
    if (db[collectionName].find(q).count()!=2) throw "should be 2";
    if (db[collectionName].find().count()!=6) throw "db should have 6 docs";
  }

  function testCopyTo() {
    reset();
    var dest = "backup";
    if (db[dest]) throw "backup collection shouldn't exist";
    if (db[collectionName].copyTo(dest)!=6) throw "should have copied all 6 docs";
    if (!db[dest]) throw "backup collection should have been created";
    if (db[dest].find().count()!=6) throw "failed to copy all content";
    if (db[collectionName].find().count()!=6) throw "original collection should still have 6 docs";
    if (db[collectionName].copyTo(dest)!=6) throw "should have copied all 6 docs";
    if (db[dest].find().count()!=6) throw "failed to copy all content";
  }

  function testDeleteOne() {
    reset();
    var q = { age : { $gt:3, $lt:7 }};
    if (db[collectionName].find(q).count()!=2) throw "should be 2";
    if (db[collectionName].find().count()!=6) throw "db should have 6 docs";
    if (db[collectionName].deleteOne(q).deletedCount!=1) throw "didn't delete single doc";
    if (db[collectionName].find(q).count()!=1) throw "should be 1 after deletion";
    if (db[collectionName].find().count()!=5) throw "db should have 5 docs in db after deleteion";
  }

  function testDeleteMany() {
    reset();
    var q = { age : { $gt:3, $lt:7 }};
    if (db[collectionName].find(q).count()!=2) throw "should be 2";
    if (db[collectionName].find().count()!=6) throw "db should have 6 docs";
    if (db[collectionName].deleteMany(q).deletedCount!=2) throw "didn't delete 2 docs";
    if (db[collectionName].find(q).count()!=0) throw "should be 0 after deletion";
    if (db[collectionName].find().count()!=4) throw "db should have 4 docs in db after deleteion";
  }

  function testDistinct() {
    reset();
    var vals = db[collectionName].distinct("age"); // [4,16,54]
    if (vals.length!=3) throw "3 distinct values of age";
    if (vals[0]!=4) throw "fail";
    if (vals[1]!=16) throw "fail";
    if (vals[2]!=54) throw "fail";
    var vals = db[collectionName].distinct("age",{legs:2}); // [54]
    if (vals.length!=1) throw "fail";
    if (vals[0]!=54) throw "fail";
  }

  function testDrop() {
    reset();
    if (db[collectionName].find().count()!=6) throw "db should have 6 docs";
    db[collectionName].drop();
    if (db[collectionName].find().count()!=0) throw "db should have no docs";

  }


  /************************************************************************
   * > db.peep.find()
   * { "_id" : ObjectId("5695be58adb5303f33363146"), "age" : 4, "legs" : 0 }
   * { "_id" : ObjectId("5695be62adb5303f33363147"), "age" : 4, "legs" : 5 }
   * { "_id" : ObjectId("5695be6aadb5303f33363148"), "age" : 54, "legs" : 2 }
   * { "_id" : ObjectId("5695be73adb5303f33363149"), "age" : 54, "legs" : 12 }
   * { "_id" : ObjectId("5695be7dadb5303f3336314a"), "name" : "steve" }
   * { "_id" : ObjectId("56983fa1396c05c1d83a87dd"), "age" : 16 }
   */
  function testFind1() {
    reset();
    var docs = testFind();
    if (docs.length!=6) throw "fail";
  }

  /************************************************************************
   * > db.peep.find({age:54,legs:2})
   * { "_id" : ObjectId("5695be6aadb5303f33363148"), "age" : 54, "legs" : 2 }
   */
  function testFind2() {
    reset();
    var docs = testFind({ age : 54,  legs: 2 });
    if (docs.length!=1) throw "fail";
  }


  /************************************************************************
   * > db.peep.find({ $and: [{ age : 54},{ legs: 2 }] })
   * { "_id" : ObjectId("5695be6aadb5303f33363148"), "age" : 54, "legs" : 2 }
   */
  function testFind3() {
    reset();
    var docs = testFind({ $and: [{ age : 54},{ legs: 2 }] });
    if (docs.length!=1) throw "fail";
  }


  /************************************************************************
   * > db.peep.find({ age: { $and: [{ $eq : 54}] }, legs: 2 })
   * Error: error: {
   *         "$err" : "Can't canonicalize query: BadValue unknown operator: $and",
   *         "code" : 17287
   */
  function testFind4() {
    reset();
    var docs = testFind({ age: { $and: [{ $eq : 54}] }, legs: 2 });
    if (docs.$err!="Can't canonicalize query: BadValue unknown operator: $and") throw "fail";    
  }


  /************************************************************************
   * > db.peep.find({ age: {$gt:3, $lt: 7}})
   * { "_id" : ObjectId("5695be58adb5303f33363146"), "age" : 4, "legs" : 0 }
   * { "_id" : ObjectId("5695be62adb5303f33363147"), "age" : 4, "legs" : 5 }
   */
  function testFind5() {
    reset();
    var docs = testFind({ age : { $gt:3, $lt:7 }});
    if (docs.length!=2) throw "fail";
  }

  /************************************************************************
   * > db.peep.find({ age: {$gt:{t:3}, $lt: 7}})
   * 
   * No Error produced by mongo (?)
   */
  function testFind6() {
    reset();
    var docs = testFind({ age: {$gt:{t:3}, $lt: 7}});
    if (docs.length!=0) throw "fail";
  }


  /************************************************************************
   * > db.peep.find({ age: {$gt:3, lt: 7}})
   * Error: error: {
   *         "$err" : "Can't canonicalize query: BadValue unknown operator: lt",
   *         "code" : 17287
   * }
   */
  function testFind7() {
    reset();
    var docs = testFind({ age: {$gt:3, lt: 7}});
    if (docs.$err!="Can't canonicalize query: BadValue unknown operator: lt") throw "fail";
  }

  /************************************************************************
   * > db.peep.find({ age: {gt:3, $lt: 7}})  // object comparison as no first '$'
   */
  function testFind8() {
    reset();
    var docs = testFind({ age: {gt:3, $lt: 7}});
    console.log(docs);
    if (docs.length!=0) throw "fail";
  }

  /************************************************************************
   * > db.peep.find({ age: {gt:3, lt: 7}}) // object comparison
   */
  function testFind9() {
     reset();
   var docs = testFind({ age: {gt:3, lt: 7}});
    if (docs.length!=0) throw "fail";
  }


  /************************************************************************
   * > db.peep.find({ age: {$gt:3, lt: 7}})
   * Error: error: {
   *         "$err" : "Can't canonicalize query: BadValue unknown operator: lt",
   *         "code" : 17287
   * }
   */
  function testFind10() {
    reset();
    var docs = testFind({ age: {$gt:3, lt: 7}});
    if (docs.$err!="Can't canonicalize query: BadValue unknown operator: lt") throw "fail";
  }
  
  /************************************************************************
   * 
   */
  function testFindArray01() {
	  reset();
      db[collectionName].insert({ scores: [4,5,6] });
      db[collectionName].insert({ scores: [3,5,7] });
	  var docs = testFind({ "scores.2" : 7});
	  if (docs.length!=1) throw "fail";
	  if (docs[0].scores[2]!=7) throw "Fail";
  }

  /************************************************************************
   * 
   */
  function testFindArray02() {
	  reset();
      db[collectionName].insert({ scores: [4,5,6] });
      db[collectionName].insert({ scores: [3,5,7] });
	  var docs = testFind({ "scores.0" : { $lt : 4 }});
	  if (docs.length!=1) throw "fail";
	  if (docs[0].scores[2]!=7) throw "Fail";
  }
  
  /************************************************************************
   * 
   */
  function testFindDocument01() {
	  reset();
	  throw "Fail";
  }

/*

> db.peep.find({age:54,legs:2},{age:1})
{ "_id" : ObjectId("5695be6aadb5303f33363148"), "age" : 54 }

> db.peep.find({age:54,legs:2},{age:0})
{ "_id" : ObjectId("5695be6aadb5303f33363148"), "legs" : 2 }

> db.peep.find({age:54,legs:2},{age:1,legs:0})
Error: error: {
        "$err" : "Can't canonicalize query: BadValue Projection cannot have a mix of inclusion and exclusion.",
        "code" : 17287
}

> db.peep.find({age:54,legs:2},{age:1,_id:1})
{ "_id" : ObjectId("5695be6aadb5303f33363148"), "age" : 54 }

> db.peep.find({age:54,legs:2},{age:0,_id:0})
{ "legs" : 2 }


*/
  function testFind_Projection() {

    function testInclusion(doc,id,age,legs) {
      if (!id != !doc._id) throw "id isn't correct";
      if (!age != !doc.age) throw "age isn't correct";
      if (!legs != !doc.legs) throw "legs isn't correct";
    }

    reset();
    var q = { age:54, legs: 2 };
    if (db[collectionName].find(q).count()!=1) throw "should be 1 doc";
    testInclusion(db[collectionName].find(q,{age:1}).next(),true,true,false);
    testInclusion(db[collectionName].find(q,{age:0}).next(),true,false,true);
    try {
      db[collectionName].find(q,{age:1,legs:0});
      throw "should have raised exception";
    } catch (e) {
      if (e.code!=17287) throw "wrong error code";
      if (e.$err!="Can't canonicalize query: BadValue Projection cannot have a mix of inclusion and exclusion.") throw "wrong error message";
    }
    testInclusion(db[collectionName].find(q,{age:1,_id:1}).next(),true,true,false);
    testInclusion(db[collectionName].find(q,{age:0,_id:0}).next(),false,false,true);
    testInclusion(db[collectionName].find(q,{}).next(),true,true,true);   
  }

  function testFindAndModify() {
    reset(); ////////////////////////////////////////////////////////////// TODO
  }

  function testFindOne() {
    reset();
    var q = { age : { $gt:3, $lt:7 }};
    if (db[collectionName].find(q).count()!=2) throw "should be 2";
    if (!db[collectionName].findOne(q)) throw "should have found 1";
    if (db[collectionName].find().count()!=6) throw "db should have 6 docs";
  }

  function testFindOne_Projection() {

    function testInclusion(doc,id,age,legs) {
      if (!id != !doc._id) throw "id isn't correct";
      if (!age != !doc.age) throw "age isn't correct";
      if (!legs != !doc.legs) throw "legs isn't correct";
    }

    reset();
    var q = { age:54, legs: 2 };
    if (!db[collectionName].findOne(q)) throw "should be 1 doc";
    testInclusion(db[collectionName].findOne(q,{age:1}),true,true,false);
    testInclusion(db[collectionName].findOne(q,{age:0}),true,false,true);
    try {
      db[collectionName].findOne(q,{age:1,legs:0});
      throw "should have raised exception";
    } catch (e) {
      if (e.code!=17287) throw "wrong error code";
      if (e.$err!="Can't canonicalize query: BadValue Projection cannot have a mix of inclusion and exclusion.") throw "wrong error message";
    }
    testInclusion(db[collectionName].findOne(q,{age:1,_id:1}),true,true,false);
    testInclusion(db[collectionName].findOne(q,{age:0,_id:0}),false,false,true);
    testInclusion(db[collectionName].findOne(q,{}),true,true,true);   
  }


  function testFindOneAndDelete() {
    reset();
    if (db[collectionName].find().count()!=6) throw "fail";
    if (db[collectionName].find({age:54}).count()!=2) throw "need 2";
    var doc = db[collectionName].findOneAndDelete({age:54});
    if (!doc) throw "didn't return deleted doc";
    if (db[collectionName].find().count()!=5) throw "fail";
  }

  function testFindOneAndDelete_NotFound() {
    reset();
    if (db[collectionName].find().count()!=6) throw "fail";
    if (db[collectionName].find({age:74}).count()!=0) throw "need 0";
    var doc = db[collectionName].findOneAndDelete({age:74});
    if (doc) throw "shouldn't have found anything to delete";
    if (db[collectionName].find().count()!=6) throw "fail";
  }

  function testFindOneAndDelete_Sort() {
    reset();
    if (db[collectionName].find().count()!=6) throw "fail";
    if (db[collectionName].find({age:54}).count()!=2) throw "need 2";
    var first = db[collectionName].findOne({age:54});

    var doc = db[collectionName].findOneAndDelete({age:54});
    if (!doc) throw "should have found something to delete";
    if (db[collectionName].find().count()!=5) throw "fail";
    if (doc._id!=first._id) throw "shoudl have deleted the first doc";

    reset();

    var doc = db[collectionName].findOneAndDelete({age:54},{ sort : { legs :1 }});
    if (!doc) throw "should have found something to delete";
    if (db[collectionName].find().count()!=5) throw "fail";
    if (doc._id==first._id) throw "shouldnt have deleted the first doc";
   
  }

  function testFindOneAndDelete_Projection() {
     reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "need 2";
    var first = db[collectionName].findOneAndDelete({age:54},{ projection : { _id: 0, legs: 0}});
    if (!first.age) throw "age should be in projection";
    if (first._id) throw "age shouldn't be included";
    if (first.legs) throw "legs shouldn't be included";
 }

  function testFindOneAndReplace() {
    reset();
    if (db[collectionName].findOne({age:76,legs:17})) throw "this shouldn't exist yet";
    var orig = db[collectionName].findOne({age:54});
    if (db[collectionName].find({age:54}).count()!=2) throw "there should be 2";
    var replaced = db[collectionName].findOneAndReplace({age:54},{age:76,legs:17});
    if (orig._id!=replaced._id) throw "replaced doc incorrect";
    if (!db[collectionName].findOne({age:76,legs:17})) throw "this doc should exist now";
    if (db[collectionName].find({age:54}).count()!=1) throw "there should only be one now";
  }

  function testFindOneAndReplace_NotFound() {
    reset();
    if (db[collectionName].findOne({age:76,legs:17})) throw "this shouldn't exist";
    if (db[collectionName].findOneAndReplace({age:76,legs:17},{})) throw "nothing should have been found to replace";
  }

  function testFindOneAndReplace_Projection() {
    reset();
    var replaced = db[collectionName].findOneAndReplace({age:54},{age:76,legs:17}, { projection : {age:0}});
    if (replaced.age) throw "age should not be in projected result";
  }

  function testFindOneAndReplace_Sort() {
    reset();

    var unsorted = db[collectionName].findOne({age:54});
    var sortOrder = 1;
    var sorted = db[collectionName].find({age:54}).sort({legs:sortOrder}).next();
    if (unsorted._id==sorted._id) {
      sortOrder = -1;
      sorted = db[collectionName].find({age:54}).sort({legs:sortOrder}).next();
      if (unsorted._id==sorted._id) throw "sorting should have returned a different doc";
    }
    var unsortedReplaced = db[collectionName].findOneAndReplace({age:54},{age:76,legs:17});
    if (unsortedReplaced._id!=unsorted._id) throw "replaced incorrect doc when not sorting";
    var sortedReplaced = db[collectionName].findOneAndReplace({age:54},{age:76,legs:17}, { sort : {legs:sortOrder}});
    if (sortedReplaced._id!=sorted._id) throw "replaced incorrect doc when sorting";
  }

  function testFindOneAndReplace_ReturnNewDocument() {
    reset();
    var orig = db[collectionName].findOne({age:54});
    var replaced = db[collectionName].findOneAndReplace({age:54},{age:76,legs:17},{returnNewDocument: false});
    if (orig._id!=replaced._id) throw "should have the returned the doc being replaced";
    var newDoc = db[collectionName].findOne({age:76,legs:17});
    var replacement = db[collectionName].findOneAndReplace({age:76,legs:17},{age:16,legs:47},{returnNewDocument: true});
    if (!replacement._id) throw "id should have been set";
    if (newDoc._id!=replacement._id) throw "the replacement/new doc should have the same id as the one replaced";
    if (replacement.age!=16) throw "doesn't appear to be the new doc (age)";
    if (replacement.legs!=47) throw "doesn't appear to be the new doc (legs)";
  }

  function testFindOneAndUpdate() {
    reset();
    var orig = db[collectionName].findOne({age:54});
    var original = db[collectionName].findOneAndUpdate({age:54},{ $inc : { age:2 }});
    if (orig._id!=original._id) throw "orig and original id's should be teh same";
    var updated = db[collectionName].findOne({_id:original._id});
    if (updated._id!=orig._id) throw "doesn't appear to be teh same doc";
    if (orig.legs!=updated.legs) throw "legs should be the same";
    if (updated.age!=56) throw "age should ahve been incremented by 2";    
  }

  function testFindOneAndUpdate_NotFound() {
    reset();
    if (db[collectionName].findOne({age:79})) throw "this shouldn't exist";
    if (db[collectionName].findOneAndUpdate({age:79},{ $inc : { age:2 }})) throw "should return null";
  }

  function testFindOneAndUpdate_Projection() {
    reset();
    var original = db[collectionName].findOneAndUpdate({age:54},{ $inc : { age:2 }},{ projection: { _id:0,legs:0 }});
    if (original._id) throw "_id shouldn't be in projection";    
    if (original.legs) throw "legs shouldn't be in projection";    
    if (!original.age) throw "age should be in projection";
    if (original.age!=54) throw "age should be 54 as per the original";
    var updated = db[collectionName].findOneAndUpdate({age:56},{ $inc : { age:2 }},{ projection: { _id:0,legs:0 },returnNewDocument: true});
    if (updated._id) throw "_id shouldn't be in projection";    
    if (updated.legs) throw "legs shouldn't be in projection";    
    if (!updated.age) throw "age should be in projection";
    if (updated.age!=58) throw "age should be 58 after updates";
  }

  function testFindOneAndUpdate_Sort() {
    reset();
    var unsorted = db[collectionName].findOne({age:54});
    var sortOrder = 1;
    var sorted = db[collectionName].find({age:54}).sort({legs:sortOrder}).next();
    if (unsorted._id==sorted._id) {
      sortOrder = -1;
      sorted = db[collectionName].find({age:54}).sort({legs:sortOrder}).next();
      if (unsorted._id==sorted._id) throw "sorting should have returned a different doc";
    }
    var unsortedUpdated = db[collectionName].findOneAndUpdate({age:54},{ $inc : { age:2 }});
    if (unsortedUpdated._id!=unsorted._id) throw "updated incorrect doc when not sorting";
    var sortedUpdated = db[collectionName].findOneAndUpdate({age:54},{ $inc : { age:2 }}, { sort : {legs:sortOrder}});
    if (sortedUpdated._id!=sorted._id) throw "updated incorrect doc when sorting";
  }

  function testFindOneAndUpdate_ReturnNewDocument() {
    reset();
    var orig = db[collectionName].findOneAndUpdate({age:54},{ $inc : { age:2 }});
    if (orig.age!=54) throw "should have returned original";
    var orig2 = db[collectionName].findOneAndUpdate({age:56},{ $inc : { age:2 }},{ returnNewDocument: false});
    if (orig2.age!=56) throw "should have returned original";
    var updated = db[collectionName].findOneAndUpdate({age:58},{ $inc : { age:2 }},{ returnNewDocument: true});
    if (updated.age!=60) throw "should have returned updated";
  }

  function testGroup() {
    reset(); ////////////////////////////////////////////////////////////// TODO
  }

  function testInsert() {
    reset();
    if (db[collectionName].find().count()!=6) throw "insert doesn't seem to be working in reset()";
  }

  function testInsertOne() {
    reset();
    if (db[collectionName].find().count()!=6) throw "insert doesn't seem to be working in reset()";
  }

  function testInsertMany() {
    reset();
    if (db[collectionName].find().count()!=6) throw "insert doesn't seem to be working in reset()";
  }

  function testMapReduce() {
    reset(); ////////////////////////////////////////////////////////////// TODO
  }

  function testReplaceOne() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs";
    var result = db[collectionName].replaceOne({age:54},{ cars : 3 });
    if (result.matchedCount!=2) throw "should have matched 2 documents";
    if (result.modifiedCount!=1) throw "should have replaced 1 document";
    var replaced = db[collectionName].findOne({cars:3});
    if (replaced.cars!=3) throw "doc doesn't look like replacement";
  }

  function testReplaceOne_NotFound() {
    reset();
    if (db[collectionName].find({age:57}).count()!=0) throw "should be no docs";
    var result = db[collectionName].replaceOne({age:57},{ cars : 3 });
    if (result.matchedCount!=0) throw "should have matched 0 documents";
    if (result.modifiedCount!=0) throw "should have replaced 0 document";
  }

  function testReplaceOne_Upsert() {
    reset();
    if (db[collectionName].find({age:57}).count()!=0) throw "should be no docs";
    var result = db[collectionName].replaceOne({age:57},{ cars : 3 },{upsert: true});
    if (result.matchedCount!=0) throw "should have matched 0 documents";
    if (result.modifiedCount!=0) throw "should have replaced 0 documents";
    if (!result.upsertedId) throw "should have created new document";
    var newDoc = db[collectionName].findOne({_id:result.upsertedId});
    if (newDoc.cars!=3) throw "new doc doesn't look like replaced doc";
  }

  function testRemove() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs";
    db[collectionName].remove({age:54});
    if (db[collectionName].find({age:54}).count()!=0) throw "should be no docs";       
  }

  function testRemove_JustOneTrue() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs";
    db[collectionName].remove({age:54},true);
    if (db[collectionName].find({age:54}).count()!=1) throw "should be 1 doc";
  }

  function testRemove_JustOneFalse() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs";
    db[collectionName].remove({age:54},false);
    if (db[collectionName].find({age:54}).count()!=0) throw "should be no docs";
  }

  function testRemove_JustOneDocTrue() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs";
    db[collectionName].remove({age:54},{ justOne : true } );
    if (db[collectionName].find({age:54}).count()!=1) throw "should be 1 doc";
  }

  function testRemove_JustOneDocFalse() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs";
    db[collectionName].remove({age:54},{ justOne : false } );
    if (db[collectionName].find({age:54}).count()!=0) throw "should be no docs";
  }

  function testUpdate() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs to start with";
    db[collectionName].update({age:54},{ $inc : { age:2 }});
    if (db[collectionName].find({age:54}).count()!=1) throw "one doc should have been updated from 54";
    if (db[collectionName].find({age:56}).count()!=1) throw "one doc should have been updated to 56";
  }

  function testUpdate_Op_Inc() {
    reset();
    var orig = db[collectionName].findOne({legs:12});
    db[collectionName].update({legs:12},{ $inc : { age:2, legs:2 }});
    var updated = db[collectionName].findOne({_id:orig._id});
    if (orig._id!=updated._id) throw "couldn't find updated doc";
    if (updated.age!=56) throw "age didn't get updated";
    if (updated.legs!=14) throw "legs didn't get updated";
  }

  function testUpdate_Op_Mul() {
    reset();
    var orig = db[collectionName].findOne({legs:12});
    db[collectionName].update({legs:12},{ $mul : { age:2, legs:2 }});
    var updated = db[collectionName].findOne({_id:orig._id});
    if (orig._id!=updated._id) throw "couldn't find updated doc";
    if (updated.age!=(54*2)) throw "age didn't get updated";
    if (updated.legs!=(12*2)) throw "legs didn't get updated";
  }

  function testUpdate_Op_Rename() {
    reset();
    var orig = db[collectionName].findOne({legs:12});
    db[collectionName].update({legs:12},{ $rename : { age:"cats", legs:"dogs" }});
    var updated = db[collectionName].findOne({_id:orig._id});
    if (orig._id!=updated._id) throw "couldn't find updated doc";
    if (updated.age) throw "age shouldnt exist";
    if (updated.legs) throw "legs shouldnt exist";
    if (updated.cats!=54) throw "cats should have value of age";
    if (updated.dogs!=12) throw "dogs should have value of legs";
  }

  function testUpdate_Op_SetOnInsert() {
    reset();
    if (db[collectionName].find({age:57}).count()!=0) throw "should be no docs to start with";
    db[collectionName].update({age:57},{ $setOnInsert: { dogs: 2, cats: 3}},{upsert:true});
    if (db[collectionName].find({dogs:2,cats:3}).count()!=1) throw "one doc should have been created";
  }

  function testUpdate_Op_Set() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs to start with";
    if (db[collectionName].find({age:54,dogs:2,cats:3}).count()!=0) throw "should be no docs";
    db[collectionName].updateMany({age:54},{ $set: { dogs: 2, cats: 3}});
    if (db[collectionName].find({age:54,dogs:2,cats:3}).count()!=2) throw "should be 2 docs";
  }

  function testUpdate_Op_Unset() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs to start with";
    db[collectionName].updateMany({age:54},{ $unset: { age: 2}});
    if (db[collectionName].find({age:54}).count()!=0) throw "no docs should be returned";
  }

  function testUpdate_Op_Min() {
    reset();
    if (db[collectionName].find({legs:10}).count()!=0) throw "should be no docs to start with";
    db[collectionName].updateMany({legs:12},{ $min: { legs: 10}});
    if (db[collectionName].find({legs:10}).count()!=1) throw "should have been udpated";
  }

  function testUpdate_Op_Max() {
    reset();
    if (db[collectionName].find({legs:24}).count()!=0) throw "should be no docs to start with";
    db[collectionName].updateMany({legs:12},{ $max: { legs: 24}});
    if (db[collectionName].find({legs:24}).count()!=1) throw "should have been udpated";
  }

  function testUpdate_Op_CurrentDate() {
    reset();
    if (db[collectionName].find({legs:12}).count()!=1) throw "should be 1 doc to start with";
    db[collectionName].updateMany({legs:12},{ $currentDate: { now: 24}});
    var doc = db[collectionName].findOne({legs:12});
    if (!doc.now) throw "now should have been set to date";
  }

  function testUpdate_Op_AddToSet() {
    reset();
    db[collectionName].insert({ me: 7, nums: [3] });
    var orig = db[collectionName].findOne({me:7});
    if (orig.nums.length!=1) throw "array of length 1 should exist";
    db[collectionName].update({me:7},{ $addToSet : { nums : [4,5] }});
    var updated = db[collectionName].findOne({me:7});
    if (updated.nums.length!=2) throw "array of length 2 should exist";
    if (updated.nums[1].length!=2) throw "array of length 2 should exist";
  }

  function testUpdate_Op_Pop() {
    reset();
    db[collectionName].insert({ me: 7, nums: [1,2,3,4,5,6] });
    db[collectionName].update({me:7},{ $pop : {nums:1} });
    var doc = db[collectionName].findOne({me:7});
    if (doc.nums.length!=5) throw "incorrect length";
    if (doc.nums[0]!=1) throw "first element should be 1";
    if (doc.nums[doc.nums.length-1]!=5) throw "last element should be 5";
    db[collectionName].update({me:7},{ $pop : {nums:-1} });
    var doc = db[collectionName].findOne({me:7});
    if (doc.nums.length!=4) throw "incorrect length";
    if (doc.nums[0]!=2) throw "first element should be 2";
    if (doc.nums[doc.nums.length-1]!=5) throw "last element should be 5";
  }

  function testUpdate_Op_PullAll() {
    reset();
    db[collectionName].insert({ me: 7, nums: [3,5,2,3,4,5,2,5] });
    db[collectionName].update({me:7},{ $pullAll : {nums:[3,5]} });
    var doc = db[collectionName].findOne({me:7});  // nums = [2,4,2]
    if (doc.nums.length!=3) throw "incorrect length";
    if (doc.nums[0]!=2) throw "[0] element should be 2";
    if (doc.nums[1]!=4) throw "[1] element should be 4";
    if (doc.nums[2]!=2) throw "[2] element should be 2";
  }

  function testUpdate_Op_Pull() {
    reset(); ////////////////////////////////////////////////////////////// TODO
  }

  function testUpdate_Op_PushAll() {
    reset();
    db[collectionName].insert({ me: 7, nums: [3] });
    db[collectionName].update({me:7},{ $pushAll : {nums:[4,5]} });
    var doc = db[collectionName].findOne({me:7});
    if (doc.nums.length!=3) throw "incorrect length";
    if (doc.nums[0]!=3) throw "[0] element should be 3";
    if (doc.nums[1]!=4) throw "[1] element should be 4";
    if (doc.nums[2]!=5) throw "[2] element should be 5";
  }

  function testUpdate_Op_Push() {
    reset();
    db[collectionName].insert({ me: 7, nums: [3] });
    db[collectionName].update({me:7},{ $push : {nums:4} });
    var doc = db[collectionName].findOne({me:7});
    if (doc.nums.length!=2) throw "incorrect length";
    if (doc.nums[0]!=3) throw "[0] element should be 3";
    if (doc.nums[1]!=4) throw "[1] element should be 4";
  }

  function testUpdate_Op_Each() {
    reset(); ////////////////////////////////////////////////////////////// TODO Update Operator Modifiers
  }

  function testUpdate_Op_Slice() {
    reset(); ////////////////////////////////////////////////////////////// TODO Update Operator Modifiers
  }

  function testUpdate_Op_Sort() {
    reset(); ////////////////////////////////////////////////////////////// TODO Update Operator Modifiers
  }

  function testUpdate_Op_Position() {
    reset(); ////////////////////////////////////////////////////////////// TODO Update Operator Modifiers
  }

  function testUpdate_Op_Bit() {
    reset();
    reset();
    db[collectionName].insert({ me: 7, val : 4 });
    db[collectionName].update({me:7},{$bit:{val: {or:3}}});
    var doc = db[collectionName].findOne({me:7});
    if (doc.val!=7) throw "4 or 3 = 7";
    db[collectionName].update({me:7},{$bit:{val: {and:14}}});
    var doc = db[collectionName].findOne({me:7});
    if (doc.val!=6) throw "7 and 14 = 6";
    db[collectionName].update({me:7},{$bit:{val: {xor:10}}});
    var doc = db[collectionName].findOne({me:7});
    if (doc.val!=12) throw "6 xor 10 = 12";
  }

  function testUpdate_Op_Isolated() {
    reset(); ////////////////////////////////////////////////////////////// TODO Update Operator Modifiers
  }


  function testUpdate_Multi() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs to start with";
    db[collectionName].update({age:54},{ $inc : { age:2 }},{multi:true});
    if (db[collectionName].find({age:54}).count()!=0) throw "all docs should have been updated from 54";
    if (db[collectionName].find({age:56}).count()!=2) throw "all docs should have been updated to 56";
  }

  function testUpdate_Upsert() {
    reset();
    if (db[collectionName].find({age:57}).count()!=0) throw "should be no docs to start with";
    db[collectionName].update({age:57},{ $inc : { age:2 }},{upsert:true});
    if (db[collectionName].find({age:59}).count()!=1) throw "one doc should have been created with age:59";
  }

  function testUpdateOne() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs to start with";
    db[collectionName].updateOne({age:54},{ $inc : { age:2 }});
    if (db[collectionName].find({age:54}).count()!=1) throw "one doc should have been updated from 54";
    if (db[collectionName].find({age:56}).count()!=1) throw "one doc should have been updated to 56";
  }

  function testUpdateOne_Upsert() {
    reset();
    if (db[collectionName].find({age:57}).count()!=0) throw "should be no docs to start with";
    db[collectionName].updateOne({age:57},{ $inc : { age:2 }},{ upsert: true});
    if (db[collectionName].find({age:59}).count()!=1) throw "new doc should have been created with age:59";
  }

  function testUpdateMany() {
    reset();
    if (db[collectionName].find({age:54}).count()!=2) throw "should be 2 docs to start with";
    db[collectionName].updateMany({age:54},{ $inc : { age:2 }});
    if (db[collectionName].find({age:54}).count()!=0) throw "these docs should have been updated from 54";
    if (db[collectionName].find({age:56}).count()!=2) throw "these docs should have been updated to 56";
  }

  function testUpdateMany_Upsert() {
    reset();
    if (db[collectionName].find({age:57}).count()!=0) throw "should be no docs to start with";
    db[collectionName].updateMany({age:57},{ $inc : { age:2 }},{ upsert: true});
    if (db[collectionName].find({age:59}).count()!=1) throw "new doc should have been created with age:59";
  }

  testCount();
  testCopyTo();
  testDeleteOne();
  testDeleteMany();
  testDistinct();
  testDrop();
  testFind1();
  testFind2();
  testFind3();
  testFind4();
  testFind5();
  testFind6();
  testFind7();
  testFind8();
  testFind9();
  testFind10();
  testFindArray01();
  testFindArray02();
  testFindDocument01();
  testFind_Projection();
  testFindAndModify();
  testFindOne();
  testFindOne_Projection();
  testFindOneAndDelete();
  testFindOneAndDelete_NotFound();
  testFindOneAndDelete_Sort();
  testFindOneAndDelete_Projection();
  testFindOneAndReplace();
  testFindOneAndReplace_NotFound();
  testFindOneAndReplace_Projection();
  testFindOneAndReplace_Sort();
  testFindOneAndReplace_ReturnNewDocument();
  testFindOneAndUpdate();
  testFindOneAndUpdate_NotFound();
  testFindOneAndUpdate_Projection();
  testFindOneAndUpdate_Sort();
  testFindOneAndUpdate_ReturnNewDocument();
  testGroup();
  testInsert()
  testInsertOne();
  testInsertMany();
  testMapReduce();
  testReplaceOne();
  testReplaceOne_NotFound();
  testReplaceOne_Upsert();
  testRemove();
  testRemove_JustOneTrue();
  testRemove_JustOneFalse();
  testRemove_JustOneDocTrue();
  testRemove_JustOneDocFalse();
  testUpdate();
  testUpdate_Op_Inc();
  testUpdate_Op_Mul();
  testUpdate_Op_Rename();
  testUpdate_Op_SetOnInsert();
  testUpdate_Op_Set();
  testUpdate_Op_Unset();
  testUpdate_Op_Min();
  testUpdate_Op_Max();
  testUpdate_Op_CurrentDate();
  testUpdate_Op_AddToSet();
  testUpdate_Op_Pop();
  testUpdate_Op_PullAll();
  testUpdate_Op_Pull();
  testUpdate_Op_PushAll();
  testUpdate_Op_Push();
  testUpdate_Op_Each();
  testUpdate_Op_Slice();
  testUpdate_Op_Sort();
  testUpdate_Op_Position();
  testUpdate_Op_Bit();
  testUpdate_Op_Isolated();
  testUpdate_Multi();
  testUpdate_Upsert();
  testUpdateOne()
  testUpdateOne_Upsert()
  testUpdateMany();
  testUpdateMany_Upsert();


} // function testCollection

function testCursor(db,collectionName) {


  function reset() {
    db.dropDatabase();
    db.createCollection(collectionName);
    db[collectionName].insert({ age: 4,  legs: 0  });
    db[collectionName].insert([{ age: 4,  legs: 5  },{ age: 54, legs: 2  }]);
    db[collectionName].insertMany([{ age: 54, legs: 12 },{ age: 16           }]);
    db[collectionName].insertOne({ name: "steve"     });
  }

  function testCount() {
    reset();
    var c = db[collectionName].find();
    if (c.count()!=6) throw "incorrect count";
    while (c.hasNext()) {
      c.next();
      if (c.count()!=6) throw "incorrect count";
    }
  }

  function testForEach() {
    reset();
    var numLegs = 0;
    var numDocs = 0;
    db[collectionName].find().forEach(function(doc) {
      numDocs++;
      if (doc.legs) numLegs += doc.legs;
    });
    if (numDocs!=6) throw "total number of docs != 6";
    if (numLegs!=19) throw "total number of legs != 19";
    
  }

  function testHasNext() {
    reset();
    var c = db[collectionName].find();
    while (c.hasNext()) {
      c.next();
    }
  }

  function testLimit() {
    reset();
    var count = 0;
    var c = db[collectionName].find().limit(3);
    while (c.hasNext()) {
      c.next();
      count++;
    }
    if (count!=3) throw "should only have max 3 when limited";
  }

  function testMap() {
    reset();
    var i = 0;
    var numDocs = 0;
    var result = db[collectionName].find().map(function(doc) {
      return i++;
    });
    if (result.length!=6) throw "result should have entry for each doc";
    if (result[2]!=2) throw "result array not correct";
  }

  function testNext() {
    reset();
    var c = db[collectionName].find();
    while (c.hasNext()) {
      c.next();
    }
  }

  function testSkip() {
    reset();
    var count = 0;
    var c = db[collectionName].find().skip(3);
    while (c.hasNext()) {
      c.next();
      count++;
    }
    if (count!=3) throw "should have skipped 3 and return 3";
  }

  function testSort() {
    var prev = 0;
    var c = db[collectionName].find({legs:{$gt:1}}).sort({legs:1});
    while (c.hasNext()) {
      var curr = c.next().legs;
      if (curr<prev) throw "should be >= than previous";
      prev = curr;
    }
    prev = 1000;
    var c = db[collectionName].find({legs:{$gt:1}}).sort({legs:-1});
    while (c.hasNext()) {
      var curr = c.next().legs;
      if (curr>prev) throw "should be <= than previous";
      prev = curr;
    }
  }

  function testToArray() {
    var c = db[collectionName].find();
    c.next();
    c.next();
    var arr = c.toArray();
    if (arr.length!=4) throw "should be 4 elements in results array";
  }




  function testSortCount() {
    reset();
    var c = db[collectionName].find().sort({legs:1});
    if (c.count()!=6) throw "incorrect count";
    while (c.hasNext()) {
      c.next();
      if (c.count()!=6) throw "incorrect count";
    }
  }

  function testSortForEach() {
    reset();
    var numLegs = 0;
    var numDocs = 0;
    db[collectionName].find().sort({legs:-1}).forEach(function(doc) {
      numDocs++;
      if (doc.legs) numLegs += doc.legs;
    });
    if (numDocs!=6) throw "total number of docs != 6";
    if (numLegs!=19) throw "total number of legs != 19"; 
  }

  function testSortHasNext() {
    reset();
    var c = db[collectionName].find().sort({legs:1});
    while (c.hasNext()) {
      c.next();
    }
  }

  function testSortLimit() {
    reset();
    var count = 0;
    var c = db[collectionName].find().sort({legs:1}).limit(3);
    while (c.hasNext()) {
      c.next();
      count++;
    }
    if (count!=3) throw "should only have max 3 when limited";
  }

  function testSortMap() {
    reset();
    var i = 0;
    var numDocs = 0;
    var result = db[collectionName].find().sort({legs:1}).map(function(doc) {
      return i++;
    });
    if (result.length!=6) throw "result should have entry for each doc";
    if (result[2]!=2) throw "result array not correct";
  }

  function testSortNext() {
    reset();
    var c = db[collectionName].find().sort({legs:1});
    while (c.hasNext()) {
      c.next();
    }
  }

  function testSortSkip() {
    reset();
    var count = 0;
    var c = db[collectionName].find().sort({legs:1}).skip(3);
    while (c.hasNext()) {
      c.next();
      count++;
    }
    if (count!=3) throw "should have skipped 3 and return 3";
  }

  function testSortSort() {
    var prev = 0;
    var c = db[collectionName].find({legs:{$gt:1}}).sort({legs:-1}).sort({legs:1});
    while (c.hasNext()) {
      var curr = c.next().legs;
      if (curr<prev) throw "should be >= than previous";
      prev = curr;
    }
    prev = 1000;
    var c = db[collectionName].find({legs:{$gt:1}}).sort({legs:1}).sort({legs:-1});
    while (c.hasNext()) {
      var curr = c.next().legs;
      if (curr>prev) throw "should be <= than previous";
      prev = curr;
    }
  }

  function testSortToArray() {
    var c = db[collectionName].find().sort({legs:1});
    c.next();
    c.next();
    var arr = c.toArray();
    if (arr.length!=4) throw "should be 4 elements in results array";
  }



  testCount();
  testForEach();
  testHasNext();
  testLimit();
  testMap();
  testNext();
  testSkip();
  testSort();
  testToArray();

  testSortCount();
  testSortForEach();
  testSortHasNext();
  testSortLimit();
  testSortMap();
  testSortNext();
  testSortSkip();
  testSortSort();
  testSortToArray();


} // function testCursor
