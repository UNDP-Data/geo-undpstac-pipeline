import asyncio
import json
import logging
from datetime import datetime
from io import StringIO
from traceback import print_exc

from azure.servicebus.aio import ServiceBusClient

logger = logging.getLogger(__name__)

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
                            msg_str = json.loads(str(msg))
                            logger.info( f"Received message: {msg_str} from queue")
                            target_date = datetime.strptime(str(msg_str), '%Y%m%d')
                            messages.append(target_date)
                            # complete the message so that the message is removed from the queue
                            await receiver.complete_message(msg)
    return messages
