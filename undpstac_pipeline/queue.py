import logging
import re
from datetime import datetime
from azure.servicebus.aio import ServiceBusClient
from undpstac_pipeline.acore import process_nighttime_data

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

    For example, "nighttime,20240201", "nighttime,20240201,force"
    """
    pattern = r'^[a-zA-Z0-9]+,\d{8}(,force)?$'
    if re.match(pattern, msg):
        split_parts = msg.split(',')
        type_name = split_parts[0]
        if type_name not in acceptable_types:
            return False

        date_string = split_parts[1]
        return check_date_format(date_string)
    else:
        return False

async def process_message_from_queue(quene_name: str,
                                     connection_string: str,
                                     lonmin: float = None,
                                     latmin: float = None,
                                     lonmax: float = None,
                                     latmax: float = None,
                                     force_processing: bool=False,
                                     ):
    """
    This funciton fetch a message from service bus queue to process a day's data for UNDP STAC
    :param quene_name: queue name of Azure Service Bus
    :param connection_string: Azure Service Bus connection string
    :return: void
    """

    # create a Service Bus client using the connection string
    async with ServiceBusClient.from_connection_string(
            conn_str=connection_string,
            logging_enable=True) as servicebus_client:
        async with servicebus_client:
            # get the Queue Receiver object for the queue
            async with servicebus_client.get_queue_receiver(queue_name=quene_name) as receiver:
                received_msgs = await receiver.receive_messages(
                    max_message_count=1,
                    max_wait_time=5,
                )
                if not received_msgs:
                    logger.info("No message in the queue.")
                else:
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
                        is_force = False
                        if force_processing is not None:
                            is_force = force_processing
                        # overwrite force_processing state if the third option in queue message is set to force.
                        elif len(parts) == 3:
                            force_string = parts[2]
                            is_force = force_string == 'force'

                        target_date = datetime.strptime(date_string, '%Y%m%d')

                        if type_name == "nighttime":
                            # flag message as completed since it cannot be locked for long time
                            await receiver.complete_message(msg)
                            logger.info(f"Start processing {str(target_date)} for {type_name}")
                            await process_nighttime_data(
                                date=target_date,
                                lonmin=lonmin, latmin=latmin, lonmax=lonmax, latmax=latmax,
                                force_processing=is_force)
                            logger.info(f"Completed processing {str(target_date)} for å{type_name}")
                        else:
                            logger.info(f"Pushing {msg} to dead-letter sub-queue")
                            await receiver.dead_letter_message(
                                msg, reason=f"invalid data type of {type_name}"
                            )
                            continue
