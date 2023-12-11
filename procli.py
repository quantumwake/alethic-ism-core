import argparse
import logging as log
import os
from typing import Dict

from numpy import safe_eval

from processor.processor_database import process_file_by_config
from processor.processor_state import find_state_files, show_state_info, StateConfigLM, \
    show_state_config_modification_info, add_state_column_value, StateDataColumnDefinition, \
    show_state_column_info, State

## TODO REMOVE THIS add it as a parameter into the cli
LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()
LOG_PATH = os.environ.get("LOG_PATH", "../logs/")
LOG_FILE = f'{LOG_PATH}/examples.log'

logging = log.getLogger(__name__)
log.basicConfig(encoding='utf-8', level=LOG_LEVEL)


def add_column_to_file(file, column: str, value: str):
    # Read the file, add a column, and save it
    print(f"Adding column {column} with value {value} to {file}")

    # state = State.load_state(file)

    if value.startswith('func:'):
        expression = value[len("func:"):]
        value = safe_eval(expression)

    print(value)


def process_file_to_database(source_file, destination):
    # Process the file and send data to the database
    print(f"Processing {source_file} to {destination}")
    # Example: Read a CSV and insert into database
    # df = pd.read_csv(source_file)
    # your_database_module.insert_data(df, destination)


def setup_state_data_commands(state_subparsers):
    # Adding 'data' subparser under 'state'
    data_parser = state_subparsers.add_parser('data', help='State data related operations')
    data_subparsers = data_parser.add_subparsers(dest='data_command')

    # add column commands under data commands
    column_parser = data_subparsers.add_parser('column', help='Manage state column information')
    column_subparsers = column_parser.add_subparsers(dest='column_action')

    # Add column add command under column commands
    column_add_parser = column_subparsers.add_parser('add', help="Add a column to the state data set")
    column_add_parser.add_argument('-c', '--column-name', required=True, help='name of the column to add')
    column_add_parser.add_argument('-v', '--column-value', required=False, help='value constant')
    column_add_parser.add_argument('-f', '--column-value-func', required=False, help="value function to evaluate")

    # Add column add command under column commands
    column_add_parser = column_subparsers.add_parser('delete', help="Delete a column to the state data set")
    column_add_parser.add_argument('-c', '--column-name')

    # Add column add command under column commands
    column_add_parser = column_subparsers.add_parser('rename', help="Rename a column in the state data set")
    column_add_parser.add_argument('-old', '--column-name')
    column_add_parser.add_argument('-new', '--new-column-name')

    # Add column add command under column commands
    column_add_parser = column_subparsers.add_parser('show', help="Show columns of the state data set")

def setup_state_config_commands(state_subparsers):
    # Adding 'config' subparser under 'state'
    config_parser = state_subparsers.add_parser('config', help='State configuration related operations')
    config_subparsers = config_parser.add_subparsers(dest='config_command')

    # Adding 'show' and 'modify' under 'config'
    show_parser = config_subparsers.add_parser('show')

    modify_parser = config_subparsers.add_parser('modify')
    modify_parser.add_argument('-v', '--new-version', required=False,
                               help='Setup new version information for this state')

    modify_parser.add_argument('-mn', '--new-model-name', required=False,
                               help='Setup new model name information for this state')

    modify_parser.add_argument('-pn', '--new-provider-name',
                               help='Setup new provider name information for this state')

    modify_parser.add_argument('-tu', '--new-user-template-path', required=False,
                               help='Setup new user template path information for this state')

    modify_parser.add_argument('-ts', '--new-system-template-path', required=False,
                               help='Setup new system template path information for this state')

    modify_parser.add_argument("--assume-yes", action="store_false", required=False,
                               dest="assume", default=None)


def setup_state_export_commands(state_subparsers):
    # Adding 'export' subparser under 'state'
    export_parser = state_subparsers.add_parser('export', help='State export related operations')
    export_subparsers = export_parser.add_subparsers(dest='export_command')

    # use a configuration file
    export_parser.add_argument("--config-file")

    # Adding 'database' and 'stream' under 'export'
    database_parser = export_subparsers.add_parser('database')
    database_parser.add_argument('--db-url')
    database_parser.add_argument('--table-name')

    stream_parser = export_subparsers.add_parser('stream')
    stream_parser.add_argument('stream_type', choices=['pulser', 'kafka'])


def setup_state_cli_commands(subparsers):
    # state main command
    state_parser = subparsers.add_parser('state', help='State related operations')
    state_subparsers = state_parser.add_subparsers(dest='state_command')
    # state_parser.add_argument('--state-file', required=False)

    setup_state_config_commands(state_subparsers=state_subparsers)
    setup_state_data_commands(state_subparsers=state_subparsers)
    setup_state_export_commands(state_subparsers=state_subparsers)

    for arg in [
        ('s', 'search-path'),
        ('r', 'search-recursive'),
        ('n', 'state-name-match'),
        ('p', 'state-path-match'),
        ('v', 'state-version-match'),
        ('mp', 'state-provider-match'),
        ('mn', 'state-model-match'),
        ('ts', 'state-system_template-match'),
        ('tu', 'state-user_template-match')
    ]: state_parser.add_argument(f'-{arg[0]}', f'--{arg[1]}')


def setup_processor_cli_commands(subparsers):
    processor_parser = subparsers.add_parser('processor', help="Processor related operations")
    processor_subparsers = processor_parser.add_subparsers(dest="processor_command")

    pass


def find_state_files_by_search_arguments(args):
    state_name_match = args.state_name_match if 'state_name_match' in args else None
    state_path_match = args.state_path_match if 'state_path_match' in args else None
    state_version_match = args.state_version_match if 'state_version_match' in args else None
    state_provider_match = args.state_provider_match if 'state_provider_match' in args else None
    state_model_match = args.state_model_match if 'state_model_match' in args else None
    state_system_template_match = args.state_system_template_match if 'state_system_template_match' in args else None
    state_user_template_match = args.state_user_template_match if 'state_user_template_match' in args else None
    search_recursive = args.search_recursive if 'search_recursive' in args else False

    return find_state_files(search_path=args.search_path,
                            search_recursive=search_recursive,
                            state_name_match=state_name_match,
                            state_path_match=state_path_match,
                            state_version_match=state_version_match,
                            state_provider_match=state_provider_match,
                            state_model_match=state_model_match,
                            state_system_template_match=state_system_template_match,
                            state_user_template_match=state_user_template_match)


def column_action_show(args: argparse.Namespace) -> Dict[str, State]:
    files = find_state_files_by_search_arguments(args=args)
    force_yes = args.force_yes if 'force_yes' in args else False

    for state_file, state in files.items():
        # display state configuration information
        show_state_info({state_file: state})
        show_state_column_info(state=state)

    return files

def column_action_rename(args: argparse.Namespace) -> Dict[str, State]:
    files = find_state_files_by_search_arguments(args=args)
    force_yes = args.force_yes if 'force_yes' in args else False

    for state_file, state in files.items():

        column_name = args.column_name if 'column_name' in args else None
        new_column_name = args.new_column_name if 'new_column_name' in args else None

        if not column_name:
            raise ValueError(f'column name must be defined')

        if not new_column_name :
            raise ValueError(f'new column name must be defined')

        # display state configuration information
        show_state_info({state_file: state})

        # if columns exist and not empty
        if force_yes:
            args.assume = "YES"
        else:
            args.assume = input(
                f'Rename column: {column_name}, new column: {new_column_name}, '
                f'type YES, otherwise enter to skip.')

        if args.assume != 'YES':
            continue

        state.columns[new_column_name] = state.columns.pop(column_name)
        if state.data and column_name in state.data:
            state.data[new_column_name] = state.data.pop(column_name)

        logging.info(f'renamed column {column_name} -> {new_column_name}')
        state.save_state(state_file)
        files[state_file] = state

    return files


def column_action_delete(args: argparse.Namespace) -> Dict[str, State]:
    files = find_state_files_by_search_arguments(args=args)
    force_yes = args.force_yes if 'force_yes' in args else False

    for state_file, state in files.items():

        column_name = args.column_name if 'column_name' in args else None

        if not column_name:
            raise ValueError(f'column name must be defined')

        # display state configuration information
        show_state_info({state_file: state})

        # if columns exist and not empty
        if force_yes:
            args.assume = "YES"
        else:
            args.assume = input(
                f'Delete column: {column_name}, '
                f'type YES, otherwise enter to skip.')

        if args.assume != 'YES':
            continue

        state.columns.pop(column_name)
        if column_name in state.data:
            state.data.pop(column_name)

        logging.info(f'deleted column {column_name}')
        state.save_state(state_file)
        files[state_file] = state

    return files


def column_action_add(args: argparse.Namespace) -> Dict[str, State]:
    files = find_state_files_by_search_arguments(args=args)
    force_yes = args.force_yes if 'force_yes' in args else False

    for state_file, state in files.items():

        column_name = args.column_name if 'column_name' in args else None
        column_value = args.column_value if 'column_value' in args else None
        column_value_func = args.column_value_func if 'column_value_func' in args else None

        if not column_name:
            raise ValueError(f'column name must be defined')

        if not column_value and not column_value_func:
            raise ValueError(f'column value and or column value function must be defined')

        # display state configuration information
        show_state_info({state_file: state})

        # if columns exist and not empty
        if force_yes:
            args.assume = "YES"
        elif state.columns and column_name in state.columns:
            args.assume = input(
                f'Overwrite {column_name} exists.'
                f'value: {column_value} for state: {state_file}: '
                f'type YES otherwise hit enter.')
        else:
            args.assume = input(
                f'Confirm addition of column: {column_name}, '
                f'value: {column_value} for state: {state_file}: '
                f'type YES otherwise hit enter.')

        if args.assume != 'YES':
            continue

        if column_value_func:
            column_value = eval(column_value_func)

        #
        state = add_state_column_value(
            state=state,
            column=StateDataColumnDefinition(
                name=column_name,
                value=column_value,  # =state.config.provider_name
                data_type="str"
            ))

        logging.debug(f'state of columns: {state.columns}')

        # persist the new state
        state.save_state(state_file)
        files[state_file] = state

    return files


def main():
    main_parser = argparse.ArgumentParser(description="Processor Command Line Interface (CLI)")
    main_command_parsers = main_parser.add_subparsers(dest='command')

    # setup state command line
    setup_state_cli_commands(main_command_parsers)
    setup_processor_cli_commands(main_command_parsers)

    # parse arguments
    args = main_parser.parse_args()

    if args.command == 'state':
        logging.debug(f'state with args {args}')
        logging.debug(f'executing command {args.command}')

        if 'config' == args.state_command:

            files = find_state_files_by_search_arguments(args=args)

            if 'show' == args.config_command:
                show_state_info(files)
            elif 'modify' == args.config_command:

                for state_file, state in files.items():
                    updated_config = show_state_config_modification_info(
                        state.config,
                        args.__dict__
                    )

                    args.assume = input('Confirm modification of state configuration: type YES otherwise hit enter.')

                    if args.assume != 'YES':
                        continue

                    # update and persist the new state
                    state.config = updated_config

                    # persist to existing output path or changed output path
                    state.save_state(output_path=state_file)

        elif 'data' == args.state_command:

            if 'column' in args.data_command:
                if 'add' in args.column_action:
                    column_action_add(args=args)
                elif 'delete' in args.column_action:
                    column_action_delete(args=args)
                elif 'rename' in args.column_action:
                    column_action_rename(args=args)
                elif 'show' in args.column_action:
                    column_action_show(args=args)
                else:
                    raise NotImplementedError(args.data_command)

        elif 'export' == args.state_command:

            files = find_state_files_by_search_arguments(args=args)

            if 'config_file' in args:
                database_url = args.db_url if 'db_url' in args else None

                process_file_by_config(states=files,
                                       config_file=args.config_file,
                                       database_url=database_url)
            else:
                raise NotImplementedError(f'Not Implemented, please use a configuration '
                                          f'file either in json or yaml format')


if __name__ == "__main__":
    main()