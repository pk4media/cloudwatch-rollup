'use strict';

var AWS = require('aws-sdk');
var HashMap = require('object-key-map');

function CloudWatchAggregation(config) {
  this._cloudWatch = new AWS.CloudWatch(config);

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
};

CloudWatchAggregation.prototype.flush = function() {
  var that = this;

  var metricName;
  for (metricName in that._aggregations) {
    that._aggregations[metricName].forEach(putMetricData);
  }

  that._aggregations = {};

  function putMetricData(dimensions, metric) {
    that._cloudWatch.putMetricData(metric, function(err, data) {
      if (err) {
        throw err;
      }
    });
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

module.exports = function(config) {
  return new CloudWatchAggregation(config);
};
