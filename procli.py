import argparse

from numpy import safe_eval

# from processor.processor_state import display_state_information
from simple_module_test import HelloWorld


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
    data_parser = state_subparsers.add_parser('data')
    data_subparsers = data_parser.add_subparsers(dest='data_command')

    # Adding 'display', 'delete', 'add column' under 'data'
    display_parser = data_subparsers.add_parser('display')
    display_parser.add_argument('filter_query')
    delete_parser = data_subparsers.add_parser('delete')
    delete_parser.add_argument('filter_query')
    add_column_parser = data_subparsers.add_parser('add_column')
    add_column_parser.add_argument('key_value', nargs='+')

def setup_state_config_commands(state_subparsers):

    # Adding 'config' subparser under 'state'
    config_parser = state_subparsers.add_parser('config')
    config_subparsers = config_parser.add_subparsers(dest='config_command')

    # Adding 'show' and 'modify' under 'config'
    show_parser = config_subparsers.add_parser('show')
    modify_parser = config_subparsers.add_parser('modify')
    for arg in [
        'name',
        'user_template_file',
        'system_template_file',
        'model_name',
        'provider_name',
        'output_path',
        'version'
    ]: modify_parser.add_argument(f'--{arg}')


def setup_state_export_commands(state_subparsers):
    # Adding 'export' subparser under 'state'
    export_parser = state_subparsers.add_parser('export')
    export_subparsers = export_parser.add_subparsers(dest='export_command')

    # Adding 'database' and 'stream' under 'export'
    database_parser = export_subparsers.add_parser('database')
    database_parser.add_argument('--db-url')
    database_parser.add_argument('--table-name')
    database_parser.add_argument('--state-file')

    stream_parser = export_subparsers.add_parser('stream')
    stream_parser.add_argument('stream_type', choices=['pulser', 'kafka'])
    stream_parser.add_argument('config_file')


def setup_state_cli_commands(subparsers):
    # state main command
    state_parser = subparsers.add_parser('state', help='State-related operations')
    state_subparsers = state_parser.add_subparsers(dest='state_command')

    setup_state_config_commands(state_subparsers=state_subparsers)
    setup_state_data_commands(state_subparsers=state_subparsers)
    setup_state_export_commands(state_subparsers=state_subparsers)


def main():
    main_parser = argparse.ArgumentParser(description="Processor Command Line Interface (CLI)")
    main_command_parsers = main_parser.add_subparsers(dest='command')

    # setup state command line
    setup_state_cli_commands(main_command_parsers)
    setup_state_config_commands(main_command_parsers)
    setup_state_data_commands(main_command_parsers)
    setup_state_export_commands(main_command_parsers)

    # parse arguments
    args = main_parser.parse_args()

    if args.command == 'state':
        print(f'state with args {args}')

        # display_state_information(args.comman)
        pass
    elif args.command == 'config':
        # Implement logic for adding column
        pass
    elif args.command == 'data':
        # Implement logic for processing file
        pass
    elif args.command == 'export':
        print(f'export state data with args {args}')

        helloworld_test = HelloWorld()
        helloworld_test.testme()
        # Implement logic for processing file
        pass


if __name__ == "__main__":


    main()

    #
    # # State command
    # state_parser = subparsers.add_parser('state', help='State-related operations')
    # state_subparsers = state_parser.add_subparsers(dest='state_command')
    #
    # # State subcommands
    # display_parser = state_subparsers.add_parser('display', help='Display state info')
    # display_parser.set_defaults(func=display_state_information())
    #
    # column_parser = state_subparsers.add_parser('column', help='Column-related operations')
    # column_parser.add_subparsers()
    # column_parser.set_defaults(func=add_column_to_file())
    #
    # # data_parser = state_subparsers.add_parser('data', help='Data operations')
    # # data_parser.set_defaults(func=show_data)
    #
    #
    # # Parse Directory Path or Filename and Filter
    # parse_cmd = subparsers.add_parser('state', help='Parse a file or directory with a filter')
    # parse_cmd.add_argument('path', type=str, help='Path to the file or directory')
    # parse_cmd.add_argument('filter', type=str, help='Filter to apply')
    #
    # # Add Column and Value to an Existing File
    # add_col_cmd = subparsers.add_parser('addcolumn', help='Add a column to a file')
    # add_col_cmd.add_argument('file', type=str, help='Path to the file')
    # add_col_cmd.add_argument('column', type=str, help='Column name')
    # add_col_cmd.add_argument('value', type=str, help='Value for the column')
    #
    # # Process File into a Database or Another Source
    # process_cmd = subparsers.add_parser('process', help='Process a file into a database or another source')
    # process_cmd.add_argument('source_file', type=str, help='Source file path')
    # process_cmd.add_argument('destination', type=str, help='Destination database or source')
