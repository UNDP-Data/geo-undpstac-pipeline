import asyncio
import json
import logging
import multiprocessing
import re
from datetime import datetime
from io import StringIO
from traceback import print_exc

from azure.servicebus.aio import AutoLockRenewer, ServiceBusClient
from undpstac_pipeline.acore import process_nighttime_data

logger = logging.getLogger(__name__)

PIPELINE_TIMEOUT = 3600
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

async def handle_lock(receiver=None, message=None, timeout_event: multiprocessing.Event = None):
    """
    Renew  the AutolockRenewer lock registered on a servicebus message.
    Long running jobs and with unpredictable execution duration  pose few chalenges.
    First, the network can be disconnected or face other issues and second the renewal operation
    can also fail or take a bit longer. For this reason  to keep an AzureService bus locked
    it is necessary to renew the lock in an infinite loop
    @param receiver, instance of Azure ServiceBusReceiver
    @param message,instance or Azure ServiceBusReceivedMessage

    @return: None
    """

    while True:
        lu = message.locked_until_utc
        n = datetime.utcnow()
        d = int((lu.replace(tzinfo=n.tzinfo) - n).total_seconds())
        # logger.debug(f'locked until {lu} utc now is {n} lock expired {message._lock_expired} and will expire in  {d}')
        if d < 10:
            logger.debug('renewing lock')
            try:

                await receiver.renew_message_lock(message=message, )
            except Exception as e:
                timeout_event.is_set()
                # it is questionable whether the exception should be propagated
                raise
        await asyncio.sleep(1)

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
                            async with AutoLockRenewer() as auto_lock_renewer:
                                auto_lock_renewer.register(
                                    receiver=receiver, renewable=msg
                                )

                                pipeline_task = asyncio.create_task(
                                    process_nighttime_data(
                                        date=target_date,
                                        lonmin=lonmin, latmin=latmin, lonmax=lonmax, latmax=latmax,
                                        force_processing=is_force
                                    )
                                )
                                pipeline_task.set_name('pipeline')

                                timeout_event = multiprocessing.Event()
                                lock_task = asyncio.create_task(
                                    handle_lock(receiver=receiver, message=msg, timeout_event=timeout_event)
                                )
                                lock_task.set_name('lock')

                                done, pending = await asyncio.wait(
                                    [lock_task, pipeline_task],
                                    return_when=asyncio.FIRST_COMPLETED,
                                    timeout=PIPELINE_TIMEOUT,
                                )
                                if len(done) == 0:
                                    error_message = f'Pipeline has timed out after {PIPELINE_TIMEOUT} seconds.'
                                    logger.error(error_message)
                                    timeout_event.set()

                                logger.debug(f'Handling done tasks')
                                for done_future in done:
                                    try:
                                        logger.info(f"Start processing {str(target_date)} for {type_name}")
                                        await done_future
                                        await receiver.complete_message(msg)
                                        logger.info(f"Completed processing {str(target_date)} for å{type_name}")

                                    except Exception as e:
                                        with StringIO() as m:
                                            print_exc(file=m)
                                            em = m.getvalue()
                                            logger.error(f'done future error {em}')

                                logger.debug(f'Cancelling pending tasks')

                                for pending_future in pending:
                                    try:
                                        pending_future.cancel()
                                        await pending_future

                                    except asyncio.exceptions.CancelledError as e:
                                        # deadletter if task_name is ingest task: name == ingest
                                        future_name = pending_future.get_name()
                                        if future_name == 'pipeline':
                                            with StringIO() as m:
                                                print_exc(file=m)
                                                em = m.getvalue()
                                                logger.error(f"Pushing message of {msg_str} to dead-letter sub-queue")
                                                await receiver.dead_letter_message(
                                                    msg, reason="pipeline task error", error_description=em
                                                )

                        else:
                            logger.info(f"Pushing {msg} to dead-letter sub-queue")
                            await receiver.dead_letter_message(
                                msg, reason=f"invalid data type of {type_name}"
                            )
                            continue
