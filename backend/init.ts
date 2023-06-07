import WebSocket from 'ws';
import serialize from 'serialize-javascript';
import _ from 'lodash';
import { ObservableDB } from './src/ObservableDB';
import { createObservableDB } from './src/createObservableDB';
import { config } from './src/config';
import { unserialize } from './src/unserialize';
import { wrapDb } from './src/wrapDb';
import { createMongoDriver } from './src/observableMongo';;

enum ConnectionType {
  collections = 'collections',
}

export const init = async (mongoDb) => {
  console.log('initing db...');
  const db = await await createObservableDB(createMongoDriver(mongoDb));

  const wrappedDb = wrapDb(db);

  async function startServer() {
    console.log('starting server...');
    const wss = new WebSocket.Server({ port: config.wsPort });

    wss.on('connection', ws => {
      let connectionType: ConnectionType;

      const closeCbs = [];
      let inited;
      ws.on('message', async data => {
        if (!connectionType) {
          connectionType = data as any;
          return;
        }

        if (connectionType == ConnectionType.collections) {
          if (!inited) {
            console.log(data.toString())
            const { collections: collectionNames, client } = unserialize(data.toString());

            const collections = {};      
            const _db: ObservableDB = db as any;
            if (collectionNames) for (const collectionName of collectionNames) {
              const observableDb = _db.createReadObserver({ meta: true });

              collections[collectionName] = await (await observableDb.collection(collectionName).find({ })).toArray();
              const dataIds = observableDb.reads();
              async function observer() {
                ws.send(serialize({ [collectionName]: await (await observableDb.collection(collectionName).find({ })).toArray() }));
              }
              for (const id of dataIds) {
                _db.observe(id, observer, client);
                closeCbs.push(() => {
                  _db.stopObserving(id, observer);
                });
              }            
            }
            console.log('done');
            ws.send(serialize(collections));
            inited = true;
          }
          else {
            const d = unserialize(data.toString());
            if (d.type == 'push') {
              const _db: ObservableDB = db as any;
              const result = await _db.taggedDb({ client: d.payload.client }).push(d.payload);
              ws.send(serialize({ _message: d.id, result }));
            }
            else if (d.type == 'batch') {
              const _db: ObservableDB = db as any;
              const result = await _db.taggedDb({ client: d.payload.client }).batch(d.payload.batch);
              ws.send(serialize({ _message: d.id, result }));
            }
          }
        }
      });
    });

  }

  return { startServer, wrappedDb };
};
