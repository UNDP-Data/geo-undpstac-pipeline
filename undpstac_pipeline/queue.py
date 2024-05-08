import logging
import re
from datetime import datetime

from azure.servicebus.aio import ServiceBusClient

logger = logging.getLogger(__name__)

acceptable_types = [
    "nighttime"
]

def check_date_format(date_string):
    """
    check date format to ensure it is a valid datetime
    """
    try:
        datetime.strptime(date_string, '%Y%m%d')
        return True
    except ValueError:
        return False

def validate_message(msg):
    """
    validate message from queue
    a message should be following the format of "{typeName},yyyyMMdd"
    typeName is a data name which want to ingest.
    yyyyMMdd is a date which will be processed.

    For example, "nighttime,20240201"
    """
    pattern = r'^[a-zA-Z0-9]+,\d{8}$'
    if re.match(pattern, msg):
        split_parts = msg.split(',')
        type_name = split_parts[0]
        if type_name not in acceptable_types:
            return False

        date_string = split_parts[1]
        return check_date_format(date_string)
    else:
        return False

async def fetch_message_from_queue(
        quene_name,
        connection_string,
):
    messages = []

    # create a Service Bus client using the connection string
    async with ServiceBusClient.from_connection_string(
            conn_str=connection_string,
            logging_enable=True) as servicebus_client:
        async with servicebus_client:
            # get the Queue Receiver object for the queue
            receiver = servicebus_client.get_queue_receiver(queue_name=quene_name)
            async with receiver:
                    received_msgs = await receiver.receive_messages(
                        max_message_count=1,
                        max_wait_time=5,
                    )
                    if received_msgs:
                        for msg in received_msgs:
                            msg_str = str(msg)
                            if not validate_message(msg_str):
                                logger.info(f"Pushing {msg} to dead-letter sub-queue")

                                await receiver.dead_letter_message(
                                    msg, reason="message does not follow specification"
                                )
                                continue

                            logger.info( f"Received message: {msg_str} from queue")

                            parts = msg_str.split(',')
                            type_name = parts[0]
                            date_string = parts[1]

                            target_date = datetime.strptime(date_string, '%Y%m%d')

                            res = {
                                "type": type_name,
                                "date": target_date
                            }

                            messages.append(res)
                            # complete the message so that the message is removed from the queue
                            await receiver.complete_message(msg)
    return messages
