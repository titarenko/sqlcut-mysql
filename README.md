# sqlcut-mysql

MySQL API adapter for [sqlcut](https://github.com/titarenko/sqlcut) module.

# API

"Promises" means "returns promise".

## query(sql, paramsArray)

Promises array of results.

```js
var sqlcut = require('sqlcut');
var db = sqlcut('sqlcut-mysql', connectionParameters);

db.query('select * from products where name = ?', ['beer']).then(console.log);
```

# License

BSD
