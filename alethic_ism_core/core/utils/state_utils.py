import logging as log

from ..processor_state import ProcessorStatus

logging = log.getLogger(__name__)


def validate_processor_state_from_created(new_status: ProcessorStatus):
    if new_status not in [ProcessorStatus.CREATED,
                          ProcessorStatus.QUEUED,
                          ProcessorStatus.RUNNING,
                          ProcessorStatus.TERMINATED,
                          ProcessorStatus.STOPPED,
                          ProcessorStatus.FAILED]:
        logging.error(
            f'unable to transition {ProcessorStatus.CREATED} to {new_status}')
        return False

    return True


def validate_processor_state_from_queued(new_status: ProcessorStatus):
    if new_status not in [ProcessorStatus.STOPPED,
                          ProcessorStatus.TERMINATED,
                          ProcessorStatus.RUNNING,
                          ProcessorStatus.QUEUED]:
        logging.error(
            f'unable to transition {ProcessorStatus.QUEUED} to {new_status}')
        return False

    return True


def validate_processor_state_from_running(new_status: ProcessorStatus):
    if new_status not in [ProcessorStatus.RUNNING,
                          ProcessorStatus.STOPPED,
                          ProcessorStatus.TERMINATED,
                          ProcessorStatus.FAILED,
                          ProcessorStatus.COMPLETED]:
        logging.error(
            f'unable to transition {ProcessorStatus.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_state_from_stopped(new_status: ProcessorStatus):
    if new_status not in [ProcessorStatus.STOPPED,
                          ProcessorStatus.TERMINATED,
                          ProcessorStatus.FAILED]:
        logging.error(
            f'unable to transition {ProcessorStatus.STOPPED} to {new_status}')
        return False

    return True


def validate_processor_state_from_terminated(new_status: ProcessorStatus):
    if new_status not in [ProcessorStatus.TERMINATED,
                          ProcessorStatus.FAILED]:
        logging.error(
            f'unable to transition {ProcessorStatus.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_state_from_failed(new_status: ProcessorStatus):
    if new_status not in [ProcessorStatus.FAILED]:
        logging.error(
            f'unable to transition {ProcessorStatus.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_state_from_completed(new_status: ProcessorStatus):
    if new_status not in [ProcessorStatus.COMPLETED,
                          ProcessorStatus.FAILED]:
        logging.error(
            f'unable to transition {ProcessorStatus.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_status_change(current_status: ProcessorStatus,
                                     new_status: ProcessorStatus):

    # if the current state is not set then create a new one
    if not current_status:
        return True

    # start with an illegal transition state and validate transition states
    if current_status in [ProcessorStatus.CREATED]:
        return validate_processor_state_from_created(
            new_status=new_status)
    elif current_status in [ProcessorStatus.QUEUED]:
        return validate_processor_state_from_queued(
            new_status=new_status)
    elif current_status in [ProcessorStatus.RUNNING]:
        return validate_processor_state_from_running(
            new_status=new_status)
    elif current_status in [ProcessorStatus.TERMINATED]:
        return validate_processor_state_from_terminated(
            new_status=new_status)
    elif current_status in [ProcessorStatus.STOPPED]:
        return validate_processor_state_from_stopped(
            new_status=new_status)
    elif current_status in [ProcessorStatus.FAILED]:
        return validate_processor_state_from_failed(
            new_status=new_status)
    elif current_status in [ProcessorStatus.COMPLETED]:
        return validate_processor_state_from_completed(
            new_status=new_status)

    # else all state transitions are exhausted raise an exception
    error = f'unable to transition from {current_status} to {new_status}, invalid transition state'
    logging.error(error)
    raise PermissionError(error)
