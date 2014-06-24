var mysql = require('mysql');
var Q = require('q');

function SqlError (args, error) {
	this.name = 'SqlError';
	this.args = args;
	this.details = error;
	this.message = 'Can\'t complete SQL query due to error.';
}

function ctor (connectionParameters) {
	var pool;

	function getPool () {
		return pool || (pool = mysql.createPool(connectionParameters));
	}

	return function (query, params) {
		var isInsert = query.indexOf('insert into') == 0;

		var deferred = Q.defer();
		getPool().query(query, params, function (error, results) {
			if (error) {
				return deferred.reject(new SqlError([query, params], error));
			}
			deferred.resolve(results);
		});

		var result = deferred.promise;

		if (isInsert) {
			result = result.then(function (rows) {
				return rows.insertId;
			});
		}

		return result;
	};
}

module.exports = ctor;
