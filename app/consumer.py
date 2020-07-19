from json import JSONDecodeError
from marshmallow import Schema, fields
import nsq


class MessageSchema(Schema):
    user = fields.Str()
    origin = fields.Str()
    destination = fields.Str()


ORIGIN = {}
DESTINATION = {}
USERS = {}


def occurrence(value, value_type):
    if value in value_type.keys():
        value_type[value] += 1
        return value_type[value]
    value_type[value] = 1
    return 1


def handler(message):
    schema = MessageSchema()
    try:
        result = schema.loads(message.body.decode())
        occurrence(result['user'], USERS)
        occurrence(result['destination'], DESTINATION)
        occurrence(result['origin'], ORIGIN)
        print(USERS)
        print(DESTINATION)
        print(ORIGIN)

        return True
    except JSONDecodeError:
        return False


r = nsq.Reader(message_handler=handler, nsqd_tcp_addresses='127.0.0.1:4150',
               topic='nsq_example', channel='chanel', lookupd_poll_interval=15)

if __name__ == '__main__':
    nsq.run()
