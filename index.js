var mysql = require('mysql');
var Q = require('q');
var _ = require('lodash');

function SqlError (args, error) {
	this.name = 'SqlError';
	this.args = args;
	this.details = error;
	this.message = 'Can\'t complete SQL query due to error.';
}

var pool;

function getPool () {
	return pool || (pool = mysql.createPool(methods.uri));
}

function query (query, params) {
	var deferred = Q.defer();
	getPool().query(query, params, function (error, results) {
		if (error) {
			return deferred.reject(new SqlError([query, params], error));
		}
		deferred.resolve(results);
	});
	return deferred.promise;
};

function querySingle () {
	var args = Array.prototype.slice.call(arguments);
	return query.apply(this, args).then(function (rows) {
		return rows && rows[0];
	});
};

function insert (table, row) {
	var keys = _.keys(row);
	var values = _.values(row);
	var placeholders = keys.map(function () {
		return '?';
	});

	var sql = [
		'insert into', table, '(', keys.join(', '), ')',
		'values (', placeholders.join(', '), ')'
	].join(' ');

	return query(sql, values).then(function (result) {
		return result.insertId;
	});
};

function update (table, row) {
	if (!row.id) {
		throw new Error('Can\'t update row without id.');
	}

	var pairs = _.pairs(row).filter(function (pair) {
		return pair[0] != 'id';
	});
	var placeholders = pairs.map(function (pair) {
		return pair[0] + ' = ?';
	});
	var values = pairs.map(function (pair) {
		return pair[1];
	});
	values.push(row.id);

	var sql = [
		'update', table, 'set', placeholders.join(', '),
		'where id = ?'
	].join(' ');
	
	return querySingle(sql, values);
};

function find (table, id) {
	var sql = ['select * from', table, 'where id = ?'].join(' ');
	return db.query(sql, [id]);
}

function remove (table, id) {
	var sql = ['delete from', table, 'where id = ?'].join(' ');
	return db.query(sql, [id]);
}

var methods = {
	query: query,
	querySingle: querySingle,
	insert: insert,
	update: update,
	find: find,
	remove: remove
};

try {
	require.resolve('../../config');
	methods.uri = require('../../config').db.uri;
} catch (e) {
	methods.uri = process.env.MSSQL_URI || '';
}

module.exports = methods;
