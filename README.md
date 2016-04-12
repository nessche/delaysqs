# delaysqs
A simple node module to extend the SQS delay over the 15 minutes hard-limit imposed by Amazon. This is done by effectively
reading and relaying the message at 15-minute intervals until the remaining delay time is below the 15-minute limit.
In other words, if at 01:00:00 I want to send a message to the Queue with a delay of 1 hr 50 minutes, the delaysqs library
will first send the message with a delay of 900 seconds, and it will repeat at 01:15:00, 01:30:00 and so on until 02:45:00,
at that point it will send the message once more to the queue but with a `DelaySeconds` attribute of 300 seconds, so that
the message will appear in queue at 02:50:00.

The relaying process uses a custom `MessageAttribute` named `deliveryTimestamp` which contains the UNIX timestamp at
which the message should finally be consumed. Specifying the explicitly the timestamp makes it such that even if the queue is
not polled for periods of time before the delivery time, the message still has a chance of being delivered in time.

## Reliability

As SQS is an ["At-least-once" deliver system](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/DistributedQueues.html),
there is always a slight chance of the message being delivered to two message consumers. At each "relay" step therefore
we have a possibility of the message being duplicated, with each of the copy of the message continuing its own independent 
existence. Therefore it is very important that the processors that will finally handle the message check whether the action
indicated by the message has already been carried out or not.

## Costs

Polling, sending and receiving messages to/from SQS has an associated cost. Delaysqs uses long polling to minimize the
roundtrips from the server in case of no traffic. The minimum costs for monitoring an SQS with long polling (prices are
region specific and subject to change, these one reported here are for region `us-west-1` and are correct as of Apr 12th,
2016)

4320 request/days * $0.00000050 $/request = 0.0216 $/day (assuming you have already consumerd your first 1 milion requests) + data transfer fees

Due to the possibility of duplicated requests, it is advisable to use delayed messages just as triggers for specific actions,
carrying only the id of the action (and not the entire action payload), making the data transfer costs a non issue.



