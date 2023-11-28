import argparse

from numpy import safe_eval

from processor import state_cli
from processor.processor_state import State, StateDataColumnDefinition


def display_state_information(path, filter):
    # Implement your file parsing logic here
    print(f"Parsing {path} with filter {filter}")
    state_cli.display_state_information(path=path, name_filter=filter)

def add_column_to_file(file, column: str, value: str):
    # Read the file, add a column, and save it
    print(f"Adding column {column} with value {value} to {file}")

    state = State.load_state(file)

    if value.startswith('func:'):
        expression = value[len("func:"):]
        value = safe_eval(expression)

    print(value)

    # state_cli.add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name=column,
    #         value=value,
    #         data_type="str"
    #     ))


def process_file_to_database(source_file, destination):
    # Process the file and send data to the database
    print(f"Processing {source_file} to {destination}")
    # Example: Read a CSV and insert into database
    # df = pd.read_csv(source_file)
    # your_database_module.insert_data(df, destination)


def setup_state_cli_commands(subparsers):

    # level 1 of state command
    # State command
    state_parser = subparsers.add_parser('state', help='State-related operations')
    state_subparsers = state_parser.add_subparsers(dest='state_command')

    # level 2 of state commands
    # State subcommands
    display_parser = state_subparsers.add_parser('display', help='Display state info')
    display_parser.set_defaults(func=display_state_information())


def main():
    parser = argparse.ArgumentParser(description="Your CLI Tool")

    subparsers = parser.add_subparsers(dest='command')

    # setup state command line
    setup_state_cli_commands(subparsers)

    # parse arguments
    args = parser.parse_args()

    if args.command == 'state':
        state_cli.display_state_information(args.comman)
        pass
    elif args.command == 'addcolumn':
        # Implement logic for adding column
        pass
    elif args.command == 'process':
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
