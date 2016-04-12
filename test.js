var chai = require('chai');
var expect = chai.expect;
var sinon = require('sinon');
var sinonChai = require('sinon-chai');

chai.use(sinonChai);

var delaysqs = require('./index');

function messageWithDeliveryTimestamp(deliveryTimestamp) {
    return ({
        MessageId: "abcdef",
        ReceiptHandle: "12345678",
        MD5OfBody: "abba15deadbeef",
        Body: "This is the body",
        Attributes: {},
        MD5OfMessageAttributes: "abba15deadbeef",
        MessageAttributes: {
            deliveryTimestamp: {
                StringValue: deliveryTimestamp.toString(),
                DataType: "Number"
            }
        }
    });
}

describe("delaysqs", function () {

    var queueUrl = 'https://sqs.region.amazonaws.com/account/queue';
    var messageCallback, errorCallback, sqs, now;

    var noMessages = function(data, callback) {
        setTimeout(function () {
            callback(null, {});
        }, 100);
    };

    beforeEach(function () {
        messageCallback = sinon.spy();
        errorCallback = sinon.spy();
        sqs = {};
        now = Math.round(new Date().getTime() / 1000);
    });

    describe("startPolling", function() {

        it("can be called many times", function (done) {
            sqs.receiveMessage = sinon.stub().yields(null, {});
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();
            delayer.startPolling();
            process.nextTick(function () {
               delayer.stopPolling();
                expect(sqs.receiveMessage).to.have.been.calledOnce;
                done();
            });
        });

        it('should start calling receiveMessage', function (done) {

            sqs.receiveMessage = sinon.stub().yields("it's an error", null);
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();

            process.nextTick(function () {
                delayer.stopPolling();
                expect(sqs.receiveMessage).to.have.been.calledOnce;
                params = sqs.receiveMessage.getCall(0).args;
                expect(params[0]).to.have.property('QueueUrl', queueUrl);
                done();
            });
                
        });

        it('should continuously invoke receiveMessage', function (done) {
            
            sqs.receiveMessage = noMessages;
            sinon.spy(sqs,"receiveMessage");
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();

            setTimeout(function () {
                delayer.stopPolling();
                expect(sqs.receiveMessage.callCount).to.be.greaterThan(3);
                done();
            }, 400)

        });


    });

    describe("enqueueMessage", function () {

        it('should immediately notifies messages whose timestamp occurs in the past', function (done) {
            
            sqs.sendMessage = sinon.stub();
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            var body = {key1: "value1", key2: "value2" };
            delayer.enqueueMessage(body, now - 1, function (err, data) {
                expect(err).to.be.null;
                expect(data).to.be.null;
                expect(messageCallback).to.have.been.calledOnce;
                expect(messageCallback).to.have.been.calledWith(body);
                expect(sqs.sendMessage).not.to.have.been.called;
                done();
            });


        });

        it('should send the message to the queue if the timestamp is in the future', function (done) {
            
            sqs.sendMessage = sinon.stub().yields(null, {MessageId: "abcdef"});
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            var body = {key1: "value1", key2: "value2" };
            delayer.enqueueMessage(body, now + 10, function (err, data) {
                expect(err).to.be.null;
                expect(data).to.eq("abcdef");
                expect(messageCallback).not.to.have.been.called;
                expect(sqs.sendMessage).to.have.been.calledOnce;
                done();
            });
        });

        it('should set the correct MessageAttributes', function (done) {
            
            sqs.sendMessage = sinon.stub().yields(null, {MessageId: "abcdef"});
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            var body = {key1: "value1", key2: "value2" };
            var deliveryTimestamp = now + 10;
            delayer.enqueueMessage(body, deliveryTimestamp, function (err, data) {
                var params = sqs.sendMessage.getCall(0).args;
                expect(params[0]).to.have.property('MessageAttributes');
                expect(params[0].MessageAttributes).to.have.property("deliveryTimestamp");
                expect(params[0].MessageAttributes.deliveryTimestamp).to.have.property("StringValue",deliveryTimestamp.toString());
                done();
            });

        });

        it('should set the correct delay if the message desired delay is less than 15 minutes', function (done) {
            
            sqs.sendMessage = sinon.stub().yields(null, {MessageId: "abcdef"});
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            var body = {key1: "value1", key2: "value2" };
            var deliveryTimestamp = now + 10;
            delayer.enqueueMessage(body, deliveryTimestamp, function (err, data) {
                var params = sqs.sendMessage.getCall(0).args;
                expect(params[0]).to.have.property('DelaySeconds',10);
                done();
            });
        });

        it('should set the correct delay if the message desired delay is more than 15 minutes', function (done) {
            
            sqs.sendMessage = sinon.stub().yields(null, {MessageId: "abcdef"});
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            var body = {key1: "value1", key2: "value2" };
            var deliveryTimestamp = now + 1000;
            delayer.enqueueMessage(body, deliveryTimestamp, function (err, data) {
                var params = sqs.sendMessage.getCall(0).args;
                expect(params[0]).to.have.property('DelaySeconds',900);
                done();
            });
        });

        it('should report an error if send fails', function (done) {
            
            sqs.sendMessage = sinon.stub().yields('An error has occurred', null);
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            var body = {key1: "value1", key2: "value2" };
            var deliveryTimestamp = now + 1000;
            delayer.enqueueMessage(body, deliveryTimestamp, function (err, data) {
                expect(data).to.be.null;
                expect(err).to.be.eq('An error has occurred');
                done();
            });
        });

    });

    describe("Process messages", function () {

        it('should send the message back to the server if deliveryTimestamp is in the future', function (done) {
            
            var deliveryTimestamp = now + 10;
            sqs.receiveMessage = sinon.stub().yields(null, {});
            sqs.receiveMessage.onFirstCall().yields(null,{Messages: [messageWithDeliveryTimestamp(deliveryTimestamp)]});
            sqs.sendMessage = sinon.stub().yields(null, {MessageId: "fedbca"});
            sqs.deleteMessage = sinon.stub().yields(null);
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();
            process.nextTick(function () {
                delayer.stopPolling();
                expect(sqs.sendMessage).to.be.calledOnce;
                var params = sqs.sendMessage.getCall(0).args;
                expect(params[0]).to.have.property('MessageAttributes');
                expect(params[0].MessageAttributes).to.have.property('deliveryTimestamp');
                expect(params[0].MessageAttributes.deliveryTimestamp).to.have.property('StringValue', deliveryTimestamp.toString());
                expect(messageCallback).not.to.be.called;
                done();
            });
        });


        it('should call the message callback if the message deliveryTimestamp is now', function (done) {
            
            var deliveryTimestamp = now;
            sqs.receiveMessage = sinon.stub().yields(null, {});
            sqs.receiveMessage.onFirstCall().yields(null,{Messages: [messageWithDeliveryTimestamp(deliveryTimestamp)]});
            sqs.sendMessage = sinon.stub().yields(null, {MessageId: "fedbca"});
            sqs.deleteMessage = sinon.stub().yields(null);
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();
            process.nextTick(function () {
                delayer.stopPolling();
                expect(sqs.sendMessage).not.to.be.called;
                expect(messageCallback).to.be.calledOnce;
                expect(messageCallback).to.be.calledWith("This is the body");

                done();
            });
        });

        it('should notify the error callback when an error occurs while deleting a message that had expired', function (done) {
            
            var deliveryTimestamp = now;
            sqs.receiveMessage = sinon.stub().yields(null, {});
            sqs.receiveMessage.onFirstCall().yields(null,{Messages: [messageWithDeliveryTimestamp(deliveryTimestamp)]});
            sqs.sendMessage = sinon.stub().yields(null, {MessageId: "fedbca"});
            sqs.deleteMessage = sinon.stub().yields('Could not delete message', null);
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();
            process.nextTick(function () {
                delayer.stopPolling();
                expect(errorCallback).to.be.calledWith('Could not delete message');
                done();
            });
        });

        it('should call the message callback if the message deliveryTimestamp is absent', function (done) {
            
            var deliveryTimestamp = now;
            var message = messageWithDeliveryTimestamp(deliveryTimestamp);
            delete message.MessageAttributes.deliveryTimestamp;
            sqs.receiveMessage = sinon.stub().yields(null, {});
            sqs.receiveMessage.onFirstCall().yields(null,{Messages: [message]});
            sqs.sendMessage = sinon.stub().yields(null, {MessageId: "fedbca"});
            sqs.deleteMessage = sinon.stub().yields(null);
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();
            process.nextTick(function () {
                delayer.stopPolling();
                expect(sqs.sendMessage).not.to.be.called;
                expect(messageCallback).to.be.calledOnce;
                expect(messageCallback).to.be.calledWith("This is the body");

                done();
            });
        });

        it('should notify the error callback if an error occurs when resend the message to the queue', function (done) {
            
            var deliveryTimestamp = now + 10;
            sqs.receiveMessage = sinon.stub().yields(null, {});
            sqs.receiveMessage.onFirstCall().yields(null,{Messages: [messageWithDeliveryTimestamp(deliveryTimestamp)]});
            sqs.sendMessage = sinon.stub().yields('An error occurred while sending', null);
            sqs.deleteMessage = sinon.stub().yields(null);
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();
            process.nextTick(function () {
                delayer.stopPolling();
                expect(sqs.deleteMessage).not.to.be.called;
                expect(errorCallback).to.be.calledWith('An error occurred while sending');
                done();
            });
        });

        it('should notify the error callback if an error occurs when deleting the message', function (done) {
            
            var deliveryTimestamp = now + 10;
            sqs.receiveMessage = sinon.stub().yields(null, {});
            sqs.receiveMessage.onFirstCall().yields(null,{Messages: [messageWithDeliveryTimestamp(deliveryTimestamp)]});
            sqs.sendMessage = sinon.stub().yields(null, {MessageId: "kjhgfds"});
            sqs.deleteMessage = sinon.stub().yields('An error occurred while deleting the message', null);
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();
            process.nextTick(function () {
                delayer.stopPolling();
                expect(errorCallback).to.be.calledWith('An error occurred while deleting the message');
                done();
            });
        });

        it('should process all messages before polling again', function (done) {
            
            var deliveryTimestamp = now;
            sqs.receiveMessage = sinon.stub().yields(null, {});
            sqs.receiveMessage.onFirstCall().yields(null,{Messages: [messageWithDeliveryTimestamp(deliveryTimestamp),
                messageWithDeliveryTimestamp(deliveryTimestamp)]});
            sqs.sendMessage = sinon.stub().yields(null, {MessageId: "fedbca"});
            sqs.deleteMessage = sinon.stub().yields(null);
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();
            process.nextTick(function () {
                delayer.stopPolling();
                expect(sqs.receiveMessage).to.have.been.calledOnce;
                expect(messageCallback).to.have.been.calledTwice;
                done();
            });
        })


    });
    
    describe("isPolling", function () {
        
        it("should reflect the polling status", function(done) {
            
            sqs.receiveMessage = sinon.stub().yields(null, {});
            var delayer = delaysqs(sqs, queueUrl, messageCallback, errorCallback);
            delayer.startPolling();
            expect(delayer.isPolling()).to.be.true;
            delayer.stopPolling();
            expect(delayer.isPolling()).to.be.false;
            process.nextTick(function () {
                expect(sqs.receiveMessage).not.to.be.called;
               done();
            });
        })
    })

});