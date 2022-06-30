var Promise = require('bluebird'),
    AMQPClient  = require('amqp10/lib').Client,
    Policy = require('amqp10/lib').Policy,
    translator = require('amqp10/lib').translator;
const config = require('./config');
var filterOffset;
var filterOption;
if (filterOffset) {
  filterOption = {
    attach: { source: { filter: {
      'apache.org:selector-filter:string': translator(
        ['described', ['symbol', 'apache.org:selector-filter:string'], ['string', "amqp.annotation.x-opt-offset > '" + filterOffset + "'"]])
    } } }
  };
}

var settings = {
  serviceBusHost: config.ServiceBusNamespace,
  eventHubName: config.EventHubName,
  partitions: config.EventHubPartitionCount,
  SASKeyName: config.EventHubKeyName,
  SASKey: config.EventHubKey
};

if (!settings.serviceBusHost || !settings.eventHubName || !settings.SASKeyName || !settings.SASKey || !settings.partitions) {
  console.warn('Must provide either settings json file or appropriate environment variables.');
  process.exit(1);
}

var protocol = settings.protocol || 'amqps';
var serviceBusHost = settings.serviceBusHost + '.servicebus.windows.net';
if (settings.serviceBusHost.indexOf(".") !== -1) {
  serviceBusHost = settings.serviceBusHost;
}
var sasName = settings.SASKeyName;
var sasKey = settings.SASKey;
var eventHubName = settings.eventHubName;
var numPartitions = settings.partitions;

var uri = protocol + '://' + encodeURIComponent(sasName) + ':' + encodeURIComponent(sasKey) + '@' + serviceBusHost;

var recvAddr = eventHubName + '/ConsumerGroups/$default/Partitions/';

var msgVal = Math.floor(Math.random() * 1000000);

var client = new AMQPClient(Policy.EventHub);
var errorHandler = function(myIdx, rx_err) { console.warn('==> RX ERROR: ', rx_err); };
var messageHandler = function (myIdx, msg) {
  console.log('received(' + myIdx + '): ', msg.body);
  if (msg.annotations) console.log('annotations: ', msg.annotations);
};

function range(begin, end) {
  return Array.apply(null, new Array(end - begin)).map(function(_, i) { return i + begin; });
}

var createPartitionReceiver = function(curIdx, curRcvAddr, filterOption) {
  return client.createReceiver(curRcvAddr, filterOption)
    .then(function (receiver) {
      receiver.on('message', messageHandler.bind(null, curIdx));
      receiver.on('errorReceived', errorHandler.bind(null, curIdx));
    });
};

client.connect(uri)
  .then(function () {
    return Promise.all([
      Promise.map(range(0, numPartitions), function(idx) {
        return createPartitionReceiver(idx, recvAddr + idx, filterOption);
      })
    ]);
  })
  .error(function (e) {
    console.warn('connection error: ', e);
  });