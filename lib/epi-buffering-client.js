import {EpistreamClient} from './epi-client'

export class EpiBufferingClient extends EpistreamClient {
  constructor(url, writeUrl, sqlReplicaConnection, sqlMasterConnection) {
    super(url, writeUrl, sqlReplicaConnection, sqlMasterConnection);
    this.results = {};
  }

  onrow(msg) {
    if (msg.queryId === 'replica_replication_time') {
      log.info('replica is timestamped at', msg.columns[0].value);
      var replica_timestamp = new Date(msg.columns[0].value);
      var unlabeled_pending_queries = [];

      this.pending_queries.forEach((query) => {
        if(query.is_write !== undefined && query.is_read !== undefined){
          unlabeled_pending_queries.push(query);
        }
      });

      if (replica_timestamp > this.last_write_time && !unlabeled_pending_queries.length > 0) {
        log.info('replica has recovered');
        this.last_write_time = null;
        this.write_counter = 0;
      } else {
        return setTimeout(() => {
          return this.query(this.sqlReplicaConnection, 'get_replication_time.mustache', null, 'replica_replication_time');
        }, 1000);
      }
    } else if (msg.queryId === this.write_queryId) {
      log.info('write is timestamped at', msg.columns[0].value);
      this.last_write_time = new Date(msg.columns[0].value);
      if (this.write_counter === 1 && this.ws_w) {
        this.query(this.sqlReplicaConnection, 'get_replication_time.mustache', null, 'replica_replication_time');
      }
    } else {
      if(this.results[msg.queryId] && this.results[msg.queryId].currentResultSet){
        this.results[msg.queryId].currentResultSet.push(msg.columns);
      }
    }
  }

  onbeginrowset(msg) {
    var newResultSet = [];
    this.results[msg.queryId] = this.results[msg.queryId] || {resultSets: []};
    this.results[msg.queryId].currentResultSet = newResultSet;
    this.results[msg.queryId].resultSets.push(newResultSet);
  };

}

