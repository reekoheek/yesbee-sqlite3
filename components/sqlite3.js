'use strict';

const sqlite3 = require('sqlite3').verbose();

var dbs = {};
function *getDatabase(uri, options) {
  if (!dbs[uri]) {
    var name = uri.split(':').slice(1).join(':');
    var db = dbs[uri] = new sqlite3.Database(name);
    db.waitings = [];

    yield new Promise(function(resolve, reject) {
      if (options.init) {
        db.run(options.init, function(err) {
          if (err) return reject(err);

          db.initialized = true;
          db.waitings.forEach(function(r) {
            r();
          });
          resolve();
        });
      }
    });
  } else if (!dbs[uri].initialized) {
    yield new Promise(function(resolve, reject) {
      if (dbs[uri].initialized) {
        return resolve();
      }

      dbs[uri].waitings.push(resolve);
    });
  }
  return dbs[uri];
}

module.exports = function(component) {
  component.defaultOptions = {
    concurrency: -1
  };

  component.createSource = function(uri) {
    throw new Error('Component: sqlite3 cannot act as source');
  };

  component.process = function *(message, options) {
    var db = yield getDatabase(message.uri, options);

    yield new Promise(function(resolve, reject) {
      db.run(message.body, message.headers, function(err) {
        if (err) return reject(err);

        resolve();
      });
    });
  };
};