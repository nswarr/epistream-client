import {AwesomeWebSocket} from 'awesocket';
import EventEmitter from 'wolfy87-eventemitter';
import log from 'simplog';

function s4() {
  return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1);
}

function guid() {
  return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4();
};

export class EpistreamClient extends EventEmitter {

  constructor(url, writeUrl, sqlReplicaConnection, sqlMasterConnection) {
    super();
    this.url = url;
    this.writeUrl = writeUrl;
    this.sqlReplicaConnection = sqlReplicaConnection;
    this.sqlMasterConnection = sqlMasterConnection;

    this.connect();
    this.last_write_time = null;
    this.write_counter = 0;
    this.write_queryId = null;
    this.pending_queries = {};
  }

  connect() {
    let ws = this.ws = new AwesomeWebSocket(this.url);
    this.queryId = 0;
    ws.onmessage = (message) => this.onMessage(message);
    ws.onclose = () => this.onClose();
    ws.onopen = () => {
      log.debug("Epiclient connection opened");
      ws.keepAlive(60 * 1000, 'ping');
    };
    ws.onerror = (err) => {
      log.error("EpiClient socket error: ", err);
    };
    ws.onsend = this.onsend;
    if (this.writeUrl) {
      ws_w = new AwesomeWebSocket(this.writeUrl);
      ws_w.onmessage = (message) => this.onMessage(message);
      ws_w.onclose = () => this.onClose();
      ws_w.onopen = () => {
        log.debug("Epiclient connection opened (write)");
        ws_w.keepAlive(60 * 1000, 'ping');
      };
      ws_w.onerror = (err) => {
        log.error("EpiClient socket error (write): ", err);
      };
      ws_w.onsend = (msg) => {this.onsend(msg)};
    }
  }

  query(connectionName, template, data, queryId=null, force_write=false) {
    let req = {
      templateName: template,
      connectionName: connectionName,
      data: data,
      queryId: queryId || guid()
    };

    if (data) {
      req.closeOnEnd = data.closeOnEnd;
    }

    if (force_write && !this.last_write_time) {
      this.last_write_time = new Date(2050, 0);
      req.is_write = true;
    }

    this.pending_queries[req.queryId] = JSON.parse(JSON.stringify(req));
    if (this.last_write_time && this.ws_w && queryId !== 'replica_replication_time') {
      this.ws_w.forceClose = req.closeOnEnd;
      log.debug("executing query: " + template + " data:" + (JSON.stringify(data)));
      req.connectionName = this.sqlMasterConnection;
      this.ws_w.send(JSON.stringify(req));
    } else {
      this.ws.forceClose = req.closeOnEnd;
      log.debug("executing query: " + template + " data:" + (JSON.stringify(data)));
      this.ws.send(this.pending_queries[req.queryId]);
    }
  }

  onMessage(message) {
    if (message.data === 'pong') {
      return;
    }

    if ((message.type != null) && (message.type = 'message')) {
      message = message.data;
    }

    if (typeof message === 'string') {
      message = JSON.parse(message);
    }

    let handler = 'on' + message.message;
    if (this[handler]) {
      this[handler](message);
    }
  }

  onClose() {
    this.emit('close');
  }

  onrow(msg) {
    if (msg.queryId === 'replica_replication_time') {
      log.info('replica is timestamped at', msg.columns[0].value);
      var replica_timestamp = new Date(msg.columns[0].value);
      var unlabeled_pending_queries = [];
      for(let query of this.pending_queries){
        if(query.is_write !== undefined && query.is_read !== undefined){
          unlabeled_pending_queries.push(query);
        }
      }

      if (replica_timestamp > this.last_write_time && !unlabeled_pending_queries.length > 0) {
        log.info('replica has recovered');
        this.last_write_time = null;
        this.write_counter = 0;
      } else {
        setTimeout(() => {
          this.query(this.sqlReplicaConnection, 'get_replication_time.mustache', null, 'replica_replication_time');
        }, 1000);
      }
    } else if (msg.queryId === this.write_queryId) {
      log.info('write is timestamped at', msg.columns[0].value);
      this.last_write_time = new Date(msg.columns[0].value);
      if (this.write_counter === 1 && this.ws_w) {
        this.query(this.sqlReplicaConnection, 'get_replication_time.mustache', null, 'replica_replication_time');
      }
    } else {
      this.emit('row', msg);
    }
  }

  ondata(msg) {
    this.emit('data', msg);
  };

  onbeginquery(msg) {
    if (this.pending_queries[msg.queryId] && !this.pending_queries[msg.queryId].is_write) {
      this.pending_queries[msg.queryId].is_read = true;
    }
    this.emit('beginquery', msg);
  }

  onendquery(msg) {
    if (this.pending_queries[msg.queryId]) {
      if (this.pending_queries[msg.queryId].is_write && this.ws_w) {
        this.write_counter += 1;
        this.write_queryId = 'write_replication_time' + this.write_counter;
        this.query(this.sqlMasterConnection, 'get_replication_time.mustache', null, this.write_queryId);
      }
      delete this.pending_queries[msg.queryId];
    }
    this.emit('endquery', msg);
  }

  onerror(msg) {
    var query_data;
    if (msg.error === 'replicawrite') {
      log.info('eating error...nom nom');
      this.last_write_time = new Date(2050, 0);
      query_data = this.pending_queries[msg.queryId];
      query_data.is_write = true;
      log.info('replica write.  switching to master');
      this.emit('replicawrite', msg);
      this.query(this.sqlMasterConnection, query_data.templateName, query_data.data, msg.queryId);
    } else {
      this.emit('error', msg);
    }
  }

  onbeginrowset(msg)  { this.emit('beginrowset', msg); };
  onendrowset(msg)    { this.emit('endrowset', msg); };
  onsend(msg)         { this.emit('send', msg); };

  onreplicamasterwrite(msg) {
    var query_data;
    query_data = this.pending_queries[msg.queryId];
    query_data.is_write = true;
    this.pending_queries[msg.queryId] = query_data;
    if (this.write_counter === 0) {
      log.info("Master write detected.  Initial write, setting timestamp on endquery.");
    } else {
      log.info("Master write detected. Increasing timestamp on endquery.");
    }
  };
}
