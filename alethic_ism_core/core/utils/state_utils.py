import logging as log
from ..base_model import StatusCode

logging = log.getLogger(__name__)


def validate_processor_state_from_created(new_status: StatusCode):
    if new_status not in [StatusCode.CREATED,
                          StatusCode.QUEUED,
                          StatusCode.RUNNING,
                          StatusCode.TERMINATED,
                          StatusCode.STOPPED,
                          StatusCode.FAILED]:
        logging.error(
            f'unable to transition {StatusCode.CREATED} to {new_status}')
        return False

    return True


def validate_processor_state_from_queued(new_status: StatusCode):
    if new_status not in [StatusCode.STOPPED,
                          StatusCode.TERMINATED,
                          StatusCode.RUNNING,
                          StatusCode.QUEUED]:
        logging.error(
            f'unable to transition {StatusCode.QUEUED} to {new_status}')
        return False

    return True


def validate_processor_state_from_running(new_status: StatusCode):
    if new_status not in [StatusCode.RUNNING,
                          StatusCode.STOPPED,
                          StatusCode.TERMINATED,
                          StatusCode.FAILED,
                          StatusCode.COMPLETED]:
        logging.error(
            f'unable to transition {StatusCode.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_state_from_stopped(new_status: StatusCode):
    if new_status not in [StatusCode.STOPPED,
                          StatusCode.TERMINATED,
                          StatusCode.FAILED]:
        logging.error(
            f'unable to transition {StatusCode.STOPPED} to {new_status}')
        return False

    return True


def validate_processor_state_from_terminated(new_status: StatusCode):
    if new_status not in [StatusCode.TERMINATED,
                          StatusCode.FAILED]:
        logging.error(
            f'unable to transition {StatusCode.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_state_from_failed(new_status: StatusCode):
    if new_status not in [StatusCode.FAILED]:
        logging.error(
            f'unable to transition {StatusCode.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_state_from_completed(new_status: StatusCode):
    if new_status not in [StatusCode.COMPLETED,
                          StatusCode.FAILED]:
        logging.error(
            f'unable to transition {StatusCode.RUNNING} to {new_status}')
        return False

    return True


def validate_processor_status_change(current_status: StatusCode,
                                     new_status: StatusCode):

    # if the current state is not set then create a new one
    if not current_status:
        return True

    # start with an illegal transition state and validate transition states
    if current_status in [StatusCode.CREATED]:
        return validate_processor_state_from_created(
            new_status=new_status)
    elif current_status in [StatusCode.QUEUED]:
        return validate_processor_state_from_queued(
            new_status=new_status)
    elif current_status in [StatusCode.RUNNING]:
        return validate_processor_state_from_running(
            new_status=new_status)
    elif current_status in [StatusCode.TERMINATED]:
        return validate_processor_state_from_terminated(
            new_status=new_status)
    elif current_status in [StatusCode.STOPPED]:
        return validate_processor_state_from_stopped(
            new_status=new_status)
    elif current_status in [StatusCode.FAILED]:
        return validate_processor_state_from_failed(
            new_status=new_status)
    elif current_status in [StatusCode.COMPLETED]:
        return validate_processor_state_from_completed(
            new_status=new_status)

    # else all state transitions are exhausted raise an exception
    error = f'unable to transition from {current_status} to {new_status}, invalid transition state'
    logging.error(error)
    raise PermissionError(error)
