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
var sendAddr = eventHubName;

var msgVal = Math.floor(Math.random() * 1000000);

var client = new AMQPClient(Policy.EventHub);

function range(begin, end) {
  return Array.apply(null, new Array(end - begin)).map(function(_, i) { return i + begin; });
}

client.connect(uri)
  .then(function () {
    return Promise.all([
      client.createSender(sendAddr)
    ]);
  })
  .spread(function(sender, unused) {
    sender.on('errorReceived', function (tx_err) { console.warn('===> TX ERROR: ', tx_err); });

    var message = { DataString: 'From Node', DataValue: msgVal };
    var options = { annotations: { 'x-opt-partition-key': 'pk' + msgVal } };
    return sender.send(message, options).then(function (state) {
      console.log('state: ', state);
    });
  })
  .error(function (e) {
    console.warn('connection error: ', e);
  });