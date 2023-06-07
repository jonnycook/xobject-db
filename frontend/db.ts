import { XMap, XObject, XStrip, X, x } from './XObject';
import axios from 'axios';
import config from './config';
import serialize from 'serialize-javascript';
import _ from 'lodash';
import ReconnectingWebSocket from 'reconnecting-websocket';
import { connectWs } from './etc/connectWs';

export const CLIENT_ID = XObject.id();


const _messagePromises = {};
const _messageCbs = {};


const _dataPushResultCbs = {};
export function onDataPushResult(id, cb) {
  if (!_dataPushResultCbs[id]) _dataPushResultCbs[id] = [];
  _dataPushResultCbs[id].push(cb);
}

function pushComplete(id) {
  _messageCbs[id]();

  if (_dataPushResultCbs[id]) {
    for (const cb of _dataPushResultCbs[id]) {
      cb(true);
    }
    delete _dataPushResultCbs[id];
  }
}

class Transaction {
  _messages: number[] = [];

  addMessageId(id) {
    this._messages.push(id);
    if (this._messages.length == this._numChanges) {
      this._resolveStartPromise();
    }
  }

  _numChanges = 0;
  _startPromise;
  _resolveStartPromise;
  hasChanges() {
    ++ this._numChanges;
    
    if (!this._startPromise) this._startPromise = new Promise(resolve => this._resolveStartPromise = resolve);
  }

  finished(): Promise<boolean> {
    return new Promise<boolean>(async resolve => {
      await this._startPromise;
      console.log(this._messages);
      const promises = [];
      for (const id of this._messages) {
        promises.push(_messagePromises[id]);
      }
      console.log(promises);
      await Promise.all(promises);
      resolve(true);
    });
  }
}

export async function dbTransaction(block) {
  const t = wrapDbTransaction(block);
  await t.finished();
}

let _currentTransaction: Transaction;

export function wrapDbTransaction(block) {
  const t = _currentTransaction = new Transaction();
  block();
  _currentTransaction = null;
  return t;
}

function parseQuery(queryString) {
  const query = {};
  const pairs = (queryString[0] === '?' ? queryString.substr(1) : queryString).split('&');
  for (let i = 0; i < pairs.length; i++) {
    const pair = pairs[i].split('=');
    query[decodeURIComponent(pair[0])] = decodeURIComponent(pair[1] || '');
  }
  return query;
}

function GET(): any {
	return parseQuery(document.location.search);
}

export function configApiServer() {
	if (GET().port) {
		const port: string = GET().port;
		return config.apiServer.replace('9001', (parseInt(port) + 1) as any);
	}
	else {
		return config.apiServer;
	}
}

let messageNumber = 0;
export function resetMessageNumber() {
  messageNumber = 0;
}

export let db = null; 

export function mockDb(v) {
  db = XMap(v);
}

let tagCounter = 0;
const pushTimeoutTimers = {};
function pushToServer(data, action='push') {
  // return;
  data._tag = XObject.id() + '-' + (tagCounter++);
  console.log('pushToServer', data._id, data._tag, data);
  if (0) {
    axios.post(`${configApiServer()}${action}`, JSON.stringify(Object.assign({}, data, { client: CLIENT_ID })), { headers: { 'Content-Type': 'application/json' }});    
  }
  else {
    const id = messageNumber++;
    _currentTransaction?.addMessageId?.(id);

    pushTimeoutTimers[id] = setTimeout(() => {
      console.log(id, 'timed out');
      dbStatus.timedOut = true;
    }, 10000);
    
    socket.send(serialize({
      type: action, 
      payload: Object.assign({}, data, { client: CLIENT_ID, messageId: id }),
      id,
    }));

    return _messagePromises[id] = new Promise(resolve => _messageCbs[id] = resolve);
  }
}

export async function applyChanges(changes) {
  if (!inited) return;
  const mutation = changes.mutation;
  if (['set', 'unset', 'remove', 'insert'].includes(mutation.type)) {
    const doc = Collection.findById(changes.collection, changes._id);
    let obj = doc;
    for (let i = 0; i < mutation.path.length; ++ i) {
      let comp = mutation.path[i];
      if (comp[0] == '&') {
        const id = comp.substr(1);
        const index = obj.findIndex((el) => el._id == id);
        comp = index;
      }

      if (i == mutation.path.length - 1) {
        XObject.withPass({ dontSync: true }, () => {
          if (mutation.type === 'set') {
            obj[comp] = XMap(mutation.value);
          }
          else if (mutation.type === 'unset') {
            delete obj[comp];
          }
          else if (mutation.type === 'remove') {
            obj[comp].splice(obj[comp].findIndex((el) => el._id === mutation.key), 1);
          }
          else if (mutation.type == 'insert') {
            obj.splice(comp, 0, XMap(mutation.el));
          }
        });
      }
      else {
        obj = obj[comp];
      }
    }
  }
  else if (mutation.type === 'create') {
    XObject.withPass({ dontSync: true }, () => {
      db[changes.collection].push(XMap(mutation.document));
    });
  }
  else if (mutation.type === 'delete') {
    XObject.withPass({ dontSync: true }, () => {
      Collection.removeDocument(changes.collection, changes._id);
    });
  }
}

const _collectionObservers = {};
export function observeDb(collection, observer) {
  if (!_collectionObservers[collection]) _collectionObservers[collection] = [];
  _collectionObservers[collection].push(observer);
}

function fireCollectionEvent(collection, event, ...params) {
  if (_collectionObservers[collection]) {
    for (let observer of _collectionObservers[collection]) {
      observer(event, ...params);
    }
  }
}

let socket: ReconnectingWebSocket;

export const dbStatus = X({ connected: false, timedOut: false, error: false });

let ignoreCollection;
export function setIgnoreCollection(value) {
  ignoreCollection = value;
}
let inited = false;
export async function initDb(col=[], opts: any={}) {
  let data; 
  const metaData = {};
  data = await new Promise<any>(async resolve => {
    socket = await connectWs('collections', {
      onMessage(event) {
        if (!data) {
          resolve(eval(`(${event.data})`));
        }
        else {
          const data = eval(`(${event.data})`);

          if ('_message' in data) {
            pushComplete(data._message);
            clearTimeout(pushTimeoutTimers[data._message]);
            if (!data.result) {
              dbStatus.error = true;
            }
            return;
          }

          const incomingMeta = {};
          const colData = {};
          for (const col in data) {
            incomingMeta[col] = data[col][1];
            colData[col] = data[col][0];
          }

          for (const collection in colData) {
            let i = 0;
            for (const incomingDoc of colData[collection]) {
              ++i;
              const doc = db[collection].find(d => d._id == incomingDoc._id);
              if (doc) {
                for (const prop in incomingDoc) {
                  const prevMeta = metaData[collection]?.[`${incomingDoc._id}.${prop}`];
                  const meta = incomingMeta[collection]?.[`${incomingDoc._id}.${prop}`];
                  const prevClient = prevMeta?.client;
                  const client = meta?.client;

                  if (client == prevClient && client == CLIENT_ID || _.isEqual(prevMeta, meta)) {
                    continue;
                  }
 

                  XObject.withPass({ dontSync: true }, () => {
                    doc[prop] = X(incomingDoc[prop]);
                  });
                }

                for (const prop in doc) {
                  if (!(prop in incomingDoc)) {
                    XObject.withPass({ dontSync: true }, () => {
                      delete doc[prop];
                    });
                  }
                }
              }
              else {
                XObject.withPass({ dontSync: true }, () => {
                  db[collection].push(X(incomingDoc));
                });
              }
            }

            const toDelete = [];
            for (const doc of db[collection]) {
              if (metaData[collection] && Object.keys(metaData[collection]).find((key: string) => key.startsWith(doc._id + '.')) && !colData[collection].find(d => d._id == doc._id)) {
                toDelete.push(doc._id);
              }
            }

            for (const id of toDelete) {
              const index = db[collection].findIndex(d => d._id == id);
              if (index != -1) {
                XObject.withPass({ dontSync: true }, () => {
                  db[collection].splice(index, 1);
                });
              }
            }
          }


          for (const col in incomingMeta) {
            metaData[col] = incomingMeta[col];
          }
        }
      },
      onClose() {
        dbStatus.connected = false;
      }
    });
    dbStatus.connected = true;
    socket.send(JSON.stringify({ collections: col, client: CLIENT_ID }));
  });

  const colData = {};
  for (const col in data) {
    colData[col] = data[col][0];
    metaData[col] = data[col][1];
  }

  
  db = XMap(colData);
  for (const colName in db) {
    db[colName][XObject._transientSymbol] = {
      $: {
        get() {
          return id => {
            return Collection.findById(colName, id);
          }
        }
      },

      findById: {
        get() {
          return id => {
            return Collection.findById(colName, id);
          }
        }
      },
      removeById: {
        get() {
          return id => {
            Collection.removeDocument(colName, { _id: id });
          }
        }
      }
    }
  }
  db._commitBatchMutations = mutations => {
    pushToServer({ batch: mutations }, 'batch')
  }

  XObject.observe(db, null, (type, prop, value) => {
    throw new Error('shouldnt be called');
  });

  function onDocument(collection, doc) {
    const timers = {};

    if (!Collection._collections[collection]) {
      Collection._collections[collection] = {};
    }
    Collection._collections[collection][doc._id] = doc;
    fireCollectionEvent(collection, 'insert', doc);
    XObject.observe(doc, mutation => {
      fireCollectionEvent(collection, 'modify', doc, mutation);
      if (mutation.path[0] == '_id') return;
      // console.log('mutation', mutation);
      if (mutation.pass && mutation.pass.dontSync) return;

      const key = mutation.path.join('.');

      clearTimeout(timers[key]);
      if (!timers[key]) {
        _currentTransaction?.hasChanges?.();
      }


      mutation = _.cloneDeep(XStrip(mutation)); 
      timers[key] = setTimeout(() => {
        if (mutation.type == 'set') {
          delete mutation.el;
        }
        pushToServer({
          collection: collection,
          _id: doc._id,
          mutation,
        });
      }, 200);
    }, null);
  }

  function onCollection(name, collection) {
    if (!collection) console.log(name);
    collection[XObject._arrayMapSymbol] = XMap;
    for (const doc of collection) {
      onDocument(name, doc);
    }
    XObject.observe(collection, null, mutation => {
      if (mutation.type === 'insert') {
        onDocument(name, mutation.el);
        if (mutation.pass && mutation.pass.dontSync) return;
        _currentTransaction?.hasChanges?.();

        pushToServer({
          collection: name,
          mutation: {
            type: 'create',
            document: XStrip(mutation.el)
          }
        });
      }
      else if (mutation.type === 'remove') {
        console.log(name, x(mutation));
        fireCollectionEvent(name, 'remove', mutation.els[0]);
        if (mutation.pass && mutation.pass.dontSync) return;
        _currentTransaction?.hasChanges?.();

        pushToServer({
          collection: name,
          _id: mutation.els[0]._id,
          mutation: {
            type: 'delete',
          }
        });
      }
    }, true);
  }

  for (const name of Object.keys(db)) {
    if (name[0] == '_') continue;
    const collection = db[name];
    onCollection(name, collection);
  }

  inited = true;
}

export async function initLocalDb(colData) {
  db = XMap(colData);
  for (const colName in db) {
    db[colName][XObject._transientSymbol] = {
      $: {
        get() {
          return id => {
            return Collection.findById(colName, id);
          }
        }
      },

      findById: {
        get() {
          return id => {
            return Collection.findById(colName, id);
          }
        }
      },
      removeById: {
        get() {
          return id => {
            Collection.removeDocument(colName, { _id: id });
          }
        }
      }
    }
  }

  function onDocument(collection, doc) {
    const timers = {};

    if (!Collection._collections[collection]) {
      Collection._collections[collection] = {};
    }
    Collection._collections[collection][doc._id] = doc;
    fireCollectionEvent(collection, 'insert', doc);
    XObject.observe(doc, mutation => {
      fireCollectionEvent(collection, 'modify', doc, mutation);
      if (mutation.path[0] == '_id') return;
      // console.log('mutation', mutation);
      if (mutation.pass && mutation.pass.dontSync) return;

      const key = mutation.path.join('.');

      clearTimeout(timers[key]);
      if (!timers[key]) {
        _currentTransaction?.hasChanges?.();
      }


      mutation = _.cloneDeep(XStrip(mutation)); 
      timers[key] = setTimeout(() => {
        // console.log('onDocument', collection, XStrip(mutation));
        if (mutation.type == 'set') {
          delete mutation.el;
        }
      }, 200);
    }, null);
    // }, null, true);
  }

  function onCollection(name, collection) {
    if (!collection) console.log(name);
    collection[XObject._arrayMapSymbol] = XMap;
    for (const doc of collection) {
      onDocument(name, doc);
    }
    XObject.observe(collection, null, mutation => {
      // console.log('onCollection', name, mutation);
      if (mutation.type === 'insert') {
        onDocument(name, mutation.el);
        if (mutation.pass && mutation.pass.dontSync) return;
        _currentTransaction?.hasChanges?.();

      }
      else if (mutation.type === 'remove') {
        console.log(name, x(mutation));
        fireCollectionEvent(name, 'remove', mutation.els[0]);
        if (mutation.pass && mutation.pass.dontSync) return;
        _currentTransaction?.hasChanges?.();
      }
    }, true);
  }

  for (const name of Object.keys(db)) {
    if (name[0] == '_') continue;
    const collection = db[name];
    onCollection(name, collection);
  }

  inited = true;
}


const Collection = {
  _collections: {},
  removeDocument(name, doc) {
    db[name].splice(db[name].findIndex(d => d._id === doc._id), 1);
  },
  findById(name, id) {
    if (!this._collections[name]) {
      return null;
      // throw new Error(`Collection ${name} doesn't exsit`);
    }
    return this._collections[name][id];
    // return db[name].find((doc) => doc._id === id);
  }
}
