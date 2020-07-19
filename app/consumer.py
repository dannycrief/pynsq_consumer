import os
import json
from json import JSONDecodeError
from marshmallow import Schema, fields
import nsq


class MessageSchema(Schema):
    user = fields.Str()
    origin = fields.Str()
    destination = fields.Str()


TCP_ADDRESSES = [os.getenv('TCP_ADDRESSES', 'http://127.0.0.1:4150')]

USERS = {}
ORIGIN = {}
DESTINATION = {}


def occurance(value, value_type):
    if value in value_type.keys():
        value_type[value] += 1
    value_type[value] = 1


def handler(message):
    schema = MessageSchema()
    try:
        result = schema.loads(message.body.decode())
        occurance(result['user'], USERS)
        occurance(result['origin'], ORIGIN)
        occurance(result['destination'], DESTINATION)
        print("Users:", USERS)
        print("Origin:", ORIGIN)
        print("Destination:", DESTINATION)
        return True
    except JSONDecodeError:
        return False


r = nsq.Reader(
    message_handler=handler,
    nsqd_tcp_addresses=TCP_ADDRESSES,
    topic='nsq_example',
    channel='consumer_channel',
    lookupd_poll_interval=15
)

if __name__ == '__main__':
    nsq.run()
