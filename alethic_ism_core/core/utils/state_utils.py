from .ismlogging import ism_logger
from ..base_model import ProcessorStatusCode

logging = ism_logger(__name__)


def validate_processor_state_from_created(new_status: ProcessorStatusCode):
    if new_status not in [ProcessorStatusCode.CREATED,
                          ProcessorStatusCode.QUEUED,
                          ProcessorStatusCode.RUNNING,
                          ProcessorStatusCode.TERMINATE,
                          ProcessorStatusCode.STOPPED,
                          ProcessorStatusCode.FAILED]:
        logging.error(
            f'unable to transition {ProcessorStatusCode.CREATED} to {new_status}')
        return False

    return True


def validate_processor_state_from_queued(new_status: ProcessorStatusCode):
    if new_status not in [ProcessorStatusCode.STOPPED,
                          ProcessorStatusCode.TERMINATE,
                          ProcessorStatusCode.RUNNING,
                          ProcessorStatusCode.QUEUED]:
        logging.error(
            f'unable to transition {ProcessorStatusCode.QUEUED} to {new_status}')
        return False

    return True


def validate_processor_state_from_running(new_status: ProcessorStatusCode):
    if new_status not in [ProcessorStatusCode.RUNNING,
                          ProcessorStatusCode.STOPPED,
                          ProcessorStatusCode.TERMINATE,
                          ProcessorStatusCode.FAILED,
                          ProcessorStatusCode.COMPLETED]:
        logging.error(
            f'unable to transition {ProcessorStatusCode.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_state_from_stopped(new_status: ProcessorStatusCode):
    if new_status not in [ProcessorStatusCode.STOPPED,
                          ProcessorStatusCode.TERMINATE,
                          ProcessorStatusCode.FAILED]:
        logging.error(
            f'unable to transition {ProcessorStatusCode.STOPPED} to {new_status}')
        return False

    return True


def validate_processor_state_from_terminate(new_status: ProcessorStatusCode):
    if new_status not in [ProcessorStatusCode.TERMINATE,
                          ProcessorStatusCode.FAILED]:
        logging.error(
            f'unable to transition {ProcessorStatusCode.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_state_from_failed(new_status: ProcessorStatusCode):
    if new_status not in [ProcessorStatusCode.FAILED]:
        logging.error(
            f'unable to transition {ProcessorStatusCode.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_state_from_completed(new_status: ProcessorStatusCode):
    if new_status not in [ProcessorStatusCode.COMPLETED,
                          ProcessorStatusCode.FAILED]:
        logging.error(
            f'unable to transition {ProcessorStatusCode.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_status_change(current_status: ProcessorStatusCode,
                                     new_status: ProcessorStatusCode):

    # if the current state is not set then create a new one
    if not current_status:
        return True

    # start with an illegal transition state and validate transition states
    if current_status in [ProcessorStatusCode.CREATED]:
        return validate_processor_state_from_created(
            new_status=new_status)
    elif current_status in [ProcessorStatusCode.QUEUED]:
        return validate_processor_state_from_queued(
            new_status=new_status)
    elif current_status in [ProcessorStatusCode.RUNNING]:
        return validate_processor_state_from_running(
            new_status=new_status)
    elif current_status in [ProcessorStatusCode.TERMINATE]:
        return validate_processor_state_from_terminate(
            new_status=new_status)
    elif current_status in [ProcessorStatusCode.STOPPED]:
        return validate_processor_state_from_stopped(
            new_status=new_status)
    elif current_status in [ProcessorStatusCode.FAILED]:
        return validate_processor_state_from_failed(
            new_status=new_status)
    elif current_status in [ProcessorStatusCode.COMPLETED]:
        return validate_processor_state_from_completed(
            new_status=new_status)

    # else all state transitions are exhausted raise an exception
    error = f'unable to transition from {current_status} to {new_status}, invalid transition state'
    logging.error(error)
    raise PermissionError(error)
