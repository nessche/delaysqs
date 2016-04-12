'use strict';

var delaysqs = function (sqs, queueUrl, messageCallback, errorCallback) {

    var polling = false;

    var _processMessage = function (message, next) {
        var deliveryTimestamp = (message.MessageAttributes && message.MessageAttributes.deliveryTimestamp) ?
            parseInt(message.MessageAttributes.deliveryTimestamp.StringValue) : 0;
        if (deliveryTimestamp) {
            var delay = deliveryTimestamp - new Date().getTime();
            if (delay > 0) {
                _resendToQueue(message, delay, next);
            } else {
                _notifyAndDelete(message, next);
            }
        } else {
            _notifyAndDelete(message, next);
        }
    };

    var _notifyAndDelete = function (message, next) {
        messageCallback(message.Body);
        _deleteMessage(message.ReceiptHandle, next);
    };

    var _deleteMessage = function (receiptHandle, next) {
        sqs.deleteMessage({
            ReceiptHandle: receiptHandle
        }, function (err, data) {
            if (err) {
                errorCallback(err);
            }
            next();
        });
    };

    var _resendToQueue = function(message, delay, next) {

        var delayInSeconds = Math.min(Math.floor(delay / 1000), 900);
        sqs.sendMessage({
            MessageBody: message.Body,
            DelaySeconds: delayInSeconds,
            MessageAttributes: message.MessageAttributes,
            QueueUrl: queueUrl
        }, function (err, data) {
            if (err) {
                errorCallback(err);
                next();
            } else {
                console.info("Message with id %s put back to queue, deleting current instance", data.MessageId);
                _deleteMessage(message.ReceiptHandle, next);
            }
        });
    };

    var _enqueueMessage = function (payload, deliveryTimestamp, callback) {
        var delay = deliveryTimestamp - new Date().getTime();
        var delayInSeconds = Math.min(Math.floor(delay / 1000), 900);
        if (delay > 0) {
            sqs.sendMessage({
                QueueUrl: queueUrl,
                MessageBody: payload,
                DelaySeconds: delayInSeconds,
                MessageAttributes: {
                    deliveryTimestamp: {
                        DataType: "Number",
                        StringValue: deliveryTimestamp.toString()
                    }
                }
            },
            function (err, data) {
                if (err) {
                    callback(err, null);
                } else {
                    callback(null, data.MessageId);
                }
            });
        } else {
            messageCallback(payload);
            callback(null, null);
        }
    };


    var _pollQueueForMessages = function () {
        if (polling) {
            console.info("Starting the long poll for messages");
            sqs.receiveMessage({
                WaitTimeSeconds: 20,
                VisibilityTimeout: 10,
                MaxNumberOfMessages: 5,
                MessageAttributeNames: ['All'],
                QueueUrl: queueUrl

            }, function (err, data) {
                if (err) {
                    console.warn("An error occurred while retrieving messages");
                    errorCallback(err);
                    process.nextTick(_pollQueueForMessages);
                } else {
                    if (data.Messages) {
                        console.info("Received %d message(s)", data.Messages.length);
                        var completed = 0;
                        for (var i = 0; i < data.Messages.length; i++) {
                            _processMessage(data.Messages[i], function () {
                                completed++;
                                if (completed == data.Messages.length) {
                                    process.nextTick(_pollQueueForMessages);
                                }
                            });
                        }
                    } else {
                        console.info("Returning from long poll with no messages");
                        process.nextTick(_pollQueueForMessages);
                    }
                }
            });
        }
    };

    var _startPolling = function () {
        if (!polling) {
            polling = true;
            process.nextTick(_pollQueueForMessages);
        }
    };

    var _stopPolling = function () {
        polling = false;
    };

    var _isPolling = function () {
        return polling;
    };

    return ({
        startPolling: _startPolling,
        stopPolling: _stopPolling,
        enqueueMessage: _enqueueMessage,
        isPolling: _isPolling
    });

};

module.exports = delaysqs;


