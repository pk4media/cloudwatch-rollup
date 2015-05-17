'use strict';

var AWS = require('aws-sdk');
var HashMap = require('object-key-map');

function CloudWatchAggregation(config, options) {
  this._cloudWatch = new AWS.CloudWatch(config);

  this.options = options || {};
  this.options.batchCount = this.options.batchCount || 20;

  this._aggregations = {};
}

CloudWatchAggregation.prototype.push = function(name, dimensions, value, options) {
  this._aggregations[name] = this._aggregations[name] || new HashMap();

  if (typeof value === 'object' || typeof value === 'undefined') {
    options = value;
    value = dimensions;
    dimensions = [];
  }

  options = options || {};

  var metric = {
    MetricName: name,
    Dimensions: dimensions,
    Timestamp: options.timestamp || new Date(),
    Unit: options.unit || 'None',
    StatisticValues: {
      Sum: 0,
      Minimum: Number.MAX_VALUE,
      Maximum: Number.MIN_VALUE,
      SampleCount: 0
    }
  };

  if (this._aggregations[name].has(dimensions)) {
    metric = this._aggregations[name].get(dimensions);
  }

  metric.StatisticValues.Sum += value;
  metric.StatisticValues.SampleCount++;
  metric.StatisticValues.Minimum = Math.min(value, metric.StatisticValues.Minimum);
  metric.StatisticValues.Maximum = Math.max(value, metric.StatisticValues.Maximum);

  this._aggregations[name].set(dimensions, metric);
};

CloudWatchAggregation.prototype.flush = function() {
  var metrics = [];
  var currentBatch;
  var metricName;
  var batchCount = this.options.batchCount;

  for (metricName in this._aggregations) {
    metrics = metrics.concat(this._aggregations[metricName].values());
  }

  this._aggregations = {};

  while (metrics.length > 0) {
    currentBatch = metrics.splice(0, batchCount);

    this._cloudWatch.putMetricData({
      Namespace: this.options.namespace || '',
      MetricData: currentBatch
    }, errorLogger);
  }

  function errorLogger(err, data) {
    if (err) {
      throw err;
    }
  }
};

CloudWatchAggregation.prototype.start = function(interval) {
  var that = this;
  interval = interval || 10000;

  if (this._interval) {
    clearInterval(this._interval);
    delete this._interval;
  }

  this._interval = setInterval(function() {
    that.flush();
  }, interval);
};

CloudWatchAggregation.prototype.stop = function() {
  if (this._interval) {
    clearInterval(this._interval);
    delete this._interval;
  }
};

module.exports = function(config, options) {
  return new CloudWatchAggregation(config, options);
};
