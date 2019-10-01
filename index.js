import {Client} from 'pg';
import * as _ from 'lodash';

/**
 * Initialize DB Connection settings
 * You can also replace this with "const config = {imported config object}"
 */
const config = {
  host: "localhost",
  user: "db_user",
  password: "db_pass",
  database: "db_name",
  port: "5432",
  log: true
};

/**
 * Initialize database columns that could be used to filter the status of a record
 * Set dbStatusField to null if not needed
 */
const dbStatusField = 'datastateid'; // Set to null if not used
const dbStatusValues = {
  Active: null,
  Delete: null,
  Archived: null
};
/**node-pg-
 * When dbSanitizeFields is set to true, each insert/update query will validate the fields to make sure they exists in the db and then sanitize those fields.
 * @type {boolean}
 */
const dbSanitizeFields = false;

/**
 * Database class to manage database queries
 * Extends the `nodejs-postgres` module
 * @class
 */
class DB extends Client {
  /**
   * Create Database instance and a connection
   * @constructor
   */
  constructor() {
    super(config);
    this.connect().then((client) => {
      console.log('DB CONNECTED');
    });
  }

  /**
   * Start a transaction
   * @param {function} callback - Function to call after transaction starts
   * @param {boolean} transaction - Flag to determine if already in transaction
   */
  begin(callback, transaction = true) {
    if(transaction) this.query('BEGIN', callback);
    else callback();
  }

  /**
   * Abort a transaction
   * @param {function} callback - Function to call after transaction is aborted
   * @param {boolean} transaction - Flag to determine if already in transaction
   */
  abort(callback, transaction = true) {
    if(transaction) this.query('ABORT', callback);
    else callback();
  }

  /**
   * Commit a transaction
   * @param {function} callback - Function to call after transaction is committed
   * @param {boolean} transaction - Flag to determine if already in transaction
   */
  commit(callback, transaction = true) {
    if(transaction) this.query('COMMIT', callback);
    else callback();
  }

  /**
   * Pre Query Request
   * @param {string} sql - SQL command to run
   * @param {object} args - SQL args to use
   * @param {function} [callback=null] - Optional function to callback after query is ran
   * @param {string} [index=null] - index results by primary id
   */
  query(sql, args, callback = null, index = '') {
    if(typeof args === 'function') {
      callback = args;
      super.query(sql, (err, res) => {
        if(err) {
          console.log(err);
          err.args = args;
        }
        callback(err, this.buildIndex(res, index));
      });
    } else {
      const start = process.hrtime();
      const isUpdate = sql.match(/UPDATE/g);
      if(isUpdate) sql = sql.replace('SET ', 'SET modified = now(), ');
      super.query(sql, args, (err, res) => {
        const end = process.hrtime(start)[1] / 1000000000;
        if(config.log) {
          console.log('===');
          console.log('sql:', sql);
          console.log('args:', args);
          console.log('time:', end + ' seconds');
        }
        if(callback) {
          if(err) {
            err.args = args;
            console.log(err);
            callback(err, null);
          } else callback(err, this.buildIndex(res, index));
        }
      });
    }
  }

  /**
   * Takes result rows and converts them to list of objects with a column value as the index value
   * @param {object} res - results object
   * @param {string} index - the column name to index by
   * @return {object} - the indexed array of results
   */
  buildIndex(res, index) {
    if(res && index && res.rows.length && res.rows[0].hasOwnProperty(index)) {
      res.irows = {};
      const list = {};
      res.rows.forEach((val) => {
        if(!list[val[index]]) list[val[index]] = [];
        list[val[index]].push(val);
      });
      res.irows = list;
      return res;
    } else {
      res.irows = [];
      return res;
    }
  }

  /**
   * Select from a table
   * @param {string} table - Table name to select from
   * @param {object={}} filter - obj of column => value to select
   * @return {Promise}
   */
  select(table, filter = {}, orderBy = null) {
    return new Promise((resolve, reject) => {
      let fields = {...filter};
      this.sanitizeFields(table, fields).then((data) => {
        const matchFields = [];
        const values = [];
        let index = 1;
        fields = data.fields;
        for(const field in fields) {
          if(Object.prototype.hasOwnProperty.call(fields, field)) {
            matchFields.push(field + ' = $' + index);
            values.push(fields[field]);
            index++;
          }
        }
        if(!filter[dbStatusField] && dbStatusField) matchFields.push(dbStatusField + ' = ' + dbStatusValues.Active);

        let sql = `SELECT * FROM "${table}"`;
        if(matchFields.length > 0) sql += ' WHERE ' + matchFields.join(' AND ');
        if(orderBy) sql += ` ORDER BY ${orderBy}`;

        this.query(sql, values, (err, result, fields) => {
          if(err) {
            console.log(err);
            reject(new Error('Failed select()'));
          } else {
            resolve(result.rows);
          }
        });
      });
    });
  }

  /**
   * Select one record from a table
   * @param {string} table - Table name to select from
   * @param {object={}} filter - obj of column => value to select
   * @return {Promise}
   */
  selectOne(table, filter = {}) {
    return new Promise((resolve, reject) => {
      this.select(table, filter).then((rows) => {
        if(rows.length) resolve(rows[0]);
        else resolve({});
      }, (err) => {
        reject(err);
      });
    });
  }

  /**
   * A Simple Update of a db table
   * @param {string} table - Table name to update
   * @param {integer} id - id in table to update
   * @param {object} data - obj of column => value to update
   * @return {Promise}
   */
  update(table, id, data, status = false) {
    return new Promise((resolve, reject) => {
      let fields = {...data};
      const compareId = (status ? id : null);
      this.sanitizeFields(table, fields, compareId).then((data) => {
        const setFields = [];
        const values = [];
        let index = 1;
        fields = data.fields;
        for(const field in fields) {
          if(Object.prototype.hasOwnProperty.call(fields, field)) {
            setFields.push(field + ' = $' + index);
            values.push(fields[field]);
            index++;
          }
        }
        if(!fields[dbStatusField] && dbStatusField) {
          setFields.push(dbStatusField + ' = $' + (setFields.length + 1));
          values.push(dbStatusValues.Active);
        }

        if(!data.matches) {
          let sql = `UPDATE "${table}" SET ` + setFields.join(', ') + ` WHERE `;
          if(typeof id === 'object') {
            for(let key in id) {
              values.push(id[key]);
              sql += key + ' = $' + values.length;
            }
          } else {
            values.push(id);
            sql += table + 'id = $' + values.length;
          }
          sql += ' RETURNING *';

          this.query(sql, values, (err, result, fields) => {
            if(err) {
              console.log(err);
              reject(err);
            } else {
              if(status) resolve({results: result.rows, updated: true});
              else resolve(result.rows);
            }
          });
        } else {
          if(status) resolve({results: data.matches, updated: false});
          else resolve(data.matches);
        }
      }, (err) => {
        reject(err);
      });
    });
  }

  /**
   * A Simple Insert of a db table
   * @param {string} table - Table to Insert into
   * @param {object} data - obj of column => value to insert
   * @param {array | string} conflict - array of conflict fields to check
   * @param {array} conflictUpdate - array of fields to update on conflict
   * @return {Promise}
   */
  insert(table, data, conflict = null, conflictUpdate = []) {
    return new Promise((resolve, reject) => {
      let fields = {...data};
      this.sanitizeFields(table, fields).then((data) => {
        const setFields = [];
        const values = [];
        const indexes = [];
        let index = 1;
        fields = data.fields;
        for(const field in fields) {
          if(Object.prototype.hasOwnProperty.call(fields, field)) {
            setFields.push(field);
            indexes.push('$' + index);
            values.push(fields[field]);
            index++;
          }
        }

        let sql = `INSERT INTO "${table}" (` + setFields.join(',') + `) VALUES (` + indexes.join(',') + `) `;
        if(conflict) {
          if(Array.isArray(conflict) && conflict.length) {
            sql += ' ON CONFLICT (' + conflict.join(',') + ') DO UPDATE SET ';
          } else {
            sql += ' ON CONFLICT ' + conflict + ' DO UPDATE SET ';
          }
          const doUpdates = [];
          conflictUpdate.forEach((conflictField) => {
            doUpdates.push(conflictField + ' = EXCLUDED.' + conflictField);
          });
          if(dbStatusField && setFields.indexOf(dbStatusField) < 0) doUpdates.push(dbStatusField + ' = ' + dbStatusValues.Active);
          sql += doUpdates.join(', ') + ' ';
        }
        sql += 'RETURNING '+table+'id';

        this.query(sql, values, (err, result, fields) => {
          if(err) {
            console.log(err);
            reject(err);
          } else {
            resolve(result.rows[0][table+'id']);
          }
        });
      }, (err) => {
        reject(err);
      });
    });
  }

  /**
   * Verify the fields that are being used against the fields in the table
   * @param {string} table - Name of table to verify against
   * @param {object} fields - obj of column => value to validate
   * @param {integer} compareId - Check existing record of this idea to see if changes are being made
   * @return {Promise<any>}
   */
  sanitizeFields(table, fields, compareId = null) {
    return new Promise((resolve, reject) => {
      if(dbSanitizeFields) {
        this.query(`SELECT * FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '${table}'`, (err, result) => {
          if(err) {
            reject(new Error('Failed To Gather Columns'));
          } else {
            const columns = result.rows;
            let oldData = {};
            const oldFields = {};
            const waitFor = new Promise((resolve, reject) => {
              if(compareId) { // Get data of existing record with id of compareId to compare later
                const tableIdField = {};
                tableIdField[table + 'id'] = compareId;
                this.selectOne(table, tableIdField).then((res) => {
                  oldData = res;
                  resolve();
                });
              } else {
                resolve();
              }
            });

            waitFor.then(() => {
              for(const field in fields) {
                if(Object.prototype.hasOwnProperty.call(fields, field)) {
                  const colData = columns.find((col) => col.column_name == field);
                  if(colData) {
                    switch(colData.data_type) {
                      case 'bit':
                        if(fields[field]) fields[field] = '1';
                        else fields[field] = '0';
                        break;
                      case 'character varying':
                        if(fields[field] !== null) fields[field] = String(fields[field]);
                        break;
                      case 'json':
                      case 'jsonb':
                        if(typeof fields[field] !== 'string') fields[field] = JSON.stringify(fields[field]);
                        break;
                    }
                    if(compareId) {
                      oldFields[field] = oldData[field];
                    }
                  } else delete fields[field];
                }
              }
              resolve({
                fields: fields,
                matches: (compareId && _.isEqual(fields, oldFields) ? oldFields : null)
              });
            });
          }
        });
      } else resolve({
        fields: fields,
        matches: null
      });
    });
  }

  /**
   * Delete a row
   * @param {string} table - Table name to delete from
   * @param {integer} id - Primary Key ID to delete
   * @param {string} idField - Primary Key name to delete
   * @return {Promise<any>}
   */
  delete(table, id, idField = '') {
    if(idField === '') idField = table + 'id';
    return new Promise((resolve, reject) => {
      let sql = `UPDATE "${table}" SET ${dbStatusField} = ${dbStatusValues.Deleted} WHERE ${idField} = ${id}`;
      if(!dbStatusField) {
        sql = `DELETE FROM "${table}" WHERE ${idField} = ${id}`;
      }
      this.query(sql, (err) => {
        if(err) {
          reject(new Error('Failed To Delete Row'));
        } else {
          resolve(true);
        }
      });
    });
  }
}

/**
 * Init DB singleton
 */
module.exports.DB = new DB();
