////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SETTINGS
//
const SETTINGS          = require('./settings.json');
const CONNECTION        = SETTINGS.hana;
const RETRY_LIMIT       = SETTINGS.api.retry.limit;
const RETRY_INTERVAL    = SETTINGS.api.retry.interval;
const SHUTDOWN_LIMIT    = 2 * RETRY_LIMIT;





////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// REQUIRED MODULES
// 
const HANA          = require('@sap/hana-client');
const { v1: UUID }  = require('uuid')
const fetch         = require('node-fetch');
const trace         = console.log;
const traceError    = console.error;





////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PRIVATE DATA
//
const JOBS              = {};
const MESSAGE_QUEUE     = {};
var SHUTDOWN_ATTEMPT    = 0;





////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// HANA CONNECTOIN AND SHUTDOWN
//





function connect() {

    return new Promise((resolve, reject) => {

        try {

            let hdb = HANA.createConnection(CONNECTION);
                hdb.connect();
                resolve(hdb);
        }

        catch(err)
        {

            reject(err);
        }
    });
}





function execute(query) {

    return connect().then(hdb => {

        return new Promise((resolve, reject) => {

            hdb.exec(query, (err, rows) => {

                if (err) {

                    console.error("HANA CONNECTION ERROR: ", err);
                    reject(err);
                }
                else {

                    resolve(rows);
                }

                hdb.disconnect();
            })
        })
    })
}





function hdb_shutdown(on_shutdown) {

    trace('HANA: SHUTDOWN ATTEMPT:', SHUTDOWN_ATTEMPT);

    let runingQueueItems = Object.values(MESSAGE_QUEUE).filter(i => i.isRunning);

    if (++SHUTDOWN_ATTEMPT < SHUTDOWN_LIMIT) {

        if (runingQueueItems.length > 0) {

            return setTimeout(hdb_shutdown.bind(this, on_shutdown), RETRY_INTERVAL);
        }
    }

    else {

        traceError('HANA SHUTDOWN RETRY LIMIT IS EXCEEDED', {time: new Date()});
    }

    trace('HANA has been DISCONNECTED already', {time: new Date()});
    return typeof on_shutdown == 'function' && on_shutdown();
}





////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MESSAGE DELIVERY WITH AUTORETRY
//
class QueueItem {

    constructor(name, query, postQuery) {

        this.id         = UUID();
        this.name       = name;
        this.query      = query;
		this.postQuery  = postQuery;
        this.attempts   = 0;
        this.first_try  = new Date();
        this.last_try   = undefined;
        this.last_err   = undefined;
        this.success    = undefined;
        this.failure    = undefined;
        JOBS[this.id]   = this;
        MESSAGE_QUEUE[this.id] = this;

        trace('\nNEW QUERY: ', name);
    }

    get isRunning() {

        return !this.success && !this.failure && this.attempts <= RETRY_LIMIT;
    }

    get start_time() {

        return this.success && this.last_try ? this.last_try : this.first_try;
    }

    get end_time() {

        return this.success || this.failure;
    }

    get duration() {

        return this.start_time ? (this.end_time || Date.now()) - this.start_time : undefined;
    }

    get status() {

        if (!this.last_try && !this.success && !this.failure) return 'not started';
        if (this.attempts > RETRY_LIMIT) return 'failure';
        if (this.failure)   return 'failure';
        if (this.success)   return 'success';
        if (this.isRunning) return 'running';
        return 'unknonw';
    }

    get brief() {

        return {

            name        : this.name,
            status      : this.status,
            duration    : this.duration,
            start       : this.start_time,
            end         : this.end_time,
            retries     : this.attempts
        }
    }

    pull() {

        this.last_try = new Date();
        let qi = this; // this is nessesary to setup lexical context of an arrow callback function used by the hdb client

        return execute(this.query)
        .then(rows => {

            trace('HANA-QUERY-PULL: SUCCESS', { name: qi.name, id: qi.id, duration: qi.duration + ' ms', start: qi.last_try, query: qi.query, response: rows, response_length: rows.length, skip_send_phase: rows.length < 1 });
            return rows.length && qi.send(rows)

        }, err => {

            qi.last_err = err;
            qi.failure = new Date();
            traceError("HANA-QUERY-PULL: FAILED TO PULL DATA FROM HANA", { err: err, queue_item: qi });
            delete MESSAGE_QUEUE[qi.id];
            return Promise.reject({reason: "pull failure", err: err, item: qi});
        })
    }

    send(data) {

        this.last_try = new Date();
        let qi = this; // this is nessesary to setup lexical context of an arrow callback function used by the hdb client

        try {

            let headers = { 'Content-Type': 'application/json' };

            if (SETTINGS.api.auth && SETTINGS.api.auth.basic) {

                let {user, password} = SETTINGS.api.auth.basic
                headers["Authorization"] = `Basic ${( Buffer.from(`${user}:${password}`)).toString('base64')}`
            }

            return fetch(SETTINGS.api.path, {

                method  : SETTINGS.api.method,
                body    : JSON.stringify(data),
                headers : headers
            })
            .then(result => {

                qi.success = new Date();

                trace('HANA-QUERY-DISPATCH: SUCCESS', { name: qi.name, duration: qi.duration + ' ms', start: qi.last_try, retries: qi.attempts, response: result });

                delete MESSAGE_QUEUE[qi.id];
                return qi.postprocess().finally(() => ({reason: "success", item: qi}));

            }, err => {

                qi.last_err = err;

                if (qi.attempts++ < RETRY_LIMIT) {

                    traceError('HANA-QUERY-DISPATCH: ERROR', err, qi);
                    return new Promise((resolve, reject) => { 

                        setTimeout(() => qi.send(data).then(resolve, reject), RETRY_INTERVAL)
                    })
                }
                else {

                    traceError("HANA-QUERY-DISPATCH: GIVEUP", { retry: qi.attempts, limit: RETRY_LIMIT, queue_item: qi });
                    qi.failure = new Date();
                    delete MESSAGE_QUEUE[qi.id];
                    return Promise.reject({reason: "send failure (giveup)", err: err, item: qi})
                }
            })
        }

        catch(err) {

            this.last_try = err;
            this.failure = new Date();
            delete MESSAGE_QUEUE[this.id];
            traceError('HANA-QUERY-DISPATCH: FATAL ERROR', err, this);
            return Promise.reject({reason: "exception", err: err, item: qi});
        }
    }

    postprocess() {

        return this.postQuery && execute(this.postQuery);
    }
}





////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MESSAGE DISPATCHING
//
function dispatch (name, query, postSQL) {

    if (SHUTDOWN_ATTEMPT) { return };
    (new QueueItem(name, query, postSQL)).pull();
}





////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MESSAGE DISPATCHING
//
function getReport() {

    let items = Object.values(JOBS);

    return {

        RUNNING     : items.filter(j => j.isRunning).map(j => j.brief),
        FAILED      : items.filter(j => !j.isRunning && j.status != 'success').map(j => j.brief),
        SUCCESSFUL  : items.filter(j => j.status == 'success').map(j => j.brief)
    }
}





////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MODULE EXPORTS
//
module.exports.shutdown     = hdb_shutdown;
module.exports.isRunning    = ()    => Object.keys(MESSAGE_QUEUE).length > 0;
module.exports.queueLength  = ()    => Object.keys(MESSAGE_QUEUE).length;
module.exports.query        = execute;
module.exports.dispatch     = dispatch;
module.exports.report       = getReport;