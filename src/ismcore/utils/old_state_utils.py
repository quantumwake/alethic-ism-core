# import os
# import re
# from datetime import datetime as dt
# from typing import List, Any
# import logging
#
# # from warnings import deprecated
#
# from ..processor_state import (
#     State,
#     StateConfig, \
#     StateConfigLM
# )
#
#
# # @deprecated
# def find_state_files(search_path: str,
#                      search_recursive: bool = False,
#                      state_path_match: str = None,
#                      state_name_match: str = None,
#                      state_version_match: str = None,
#                      state_provider_match: str = None,
#                      state_model_match: str = None,
#                      state_user_template_match: str = None,
#                      state_system_template_match: str = None):
#     logging.debug(f'searching for state files in path {search_path}, '
#                   f'recursive: {search_recursive}, '
#                   f'state_path_match: {state_path_match}, '
#                   f'state_name_match: {state_name_match}')
#
#     if not os.path.exists(search_path):
#         raise Exception(f'state path does not exist: {search_path}')
#
#     files = os.listdir(search_path)
#
#     if not files:
#         logging.error(f'no state files found in {search_path}')
#         return
#
#     # final result set to merge with previous state result files
#     results = {}
#
#     for nodes in files:
#         final_path = f'{search_path}/{nodes}'
#
#         if os.path.isdir(final_path):
#             if search_recursive:
#                 logging.debug(f'recursive path {final_path}')
#
#                 # find states recursively
#                 new_results = find_state_files(search_path=final_path,
#                                                search_recursive=search_recursive,
#                                                state_path_match=state_path_match,
#                                                state_name_match=state_name_match,
#                                                state_version_match=state_version_match,
#                                                state_provider_match=state_provider_match,
#                                                state_model_match=state_model_match,
#                                                state_user_template_match=state_user_template_match,
#                                                state_system_template_match=state_system_template_match)
#                 if new_results:
#                     # append the recursive results to the current call level
#                     results = {**results, **new_results}
#                 else:
#                     logging.debug(f'no state files matched in path: {final_path}')
#
#             continue
#
#         def inv_check_match(search, value):
#             # attempt to match full path before opening state file
#             logging.debug(f'searching for pattern {search}, value: {value}, file: {final_path}')
#             if search and not re.match(search, value) and search not in value:
#                 logging.debug(f'search pattern: {search} not matched as per value: {value} on file {final_path}')
#                 return True
#
#             return False
#
#         # attempt to match full path before opening state file
#         if inv_check_match(state_path_match, final_path):
#             continue
#
#         # TODO must be a better way to only read the state configuration instead of the entire file
#         state = State.load_state(final_path)
#
#         # search the file path for a pattern match
#         if inv_check_match(state_name_match, state.config.name):
#             continue
#
#         # if is instance of state config lm
#         if isinstance(state.config, StateConfigLM):
#
#             # backwards compatibility, TODO remove this at some point
#             if 'version' not in state.config.__dict__:
#                 state.config.version = "Version 0.0"
#             try:
#                 if (inv_check_match(state_user_template_match, state.config.user_template_path) or
#                         inv_check_match(state_system_template_match, state.config.system_template_path) or
#                         inv_check_match(state_model_match, state.config.model_name) or
#                         inv_check_match(state_provider_match, state.config.provider_name) or
#                         inv_check_match(state_version_match, state.config.version)):
#                     continue
#             except Exception as e:
#                 logging.error(
#                     f'unable to process file {final_path}, attempting to evaluate config match {state.config}')
#                 logging.error(e)
#                 continue
#
#         elif state_user_template_match or state_system_template_match:
#             # skip the file is the template user match or system template
#             # match is defined and the config type is not StateConfigLM
#             continue
#
#         # append as a dictionary of file name paths and state values
#         results[final_path] = state
#
#     return results
#
#
# # @deprecated
# def search_state(path: str, filter_func: Any):
#     # TODO terrible way to search the state but it will have to do for now,
#     #  basically a linear search since it has to open the entire state file
#     #  and iterate through it.
#     state = State.load_state(path)
#     query_states = [state.get_query_state_from_row_index(x) for x in range(state.count)]
#     return [query_state for query_state in query_states if filter_func(query_state)]
#
#
# # @deprecated
# def show_state_config_modification_info(
#         old_config: StateConfig,
#         new_config: StateConfig):
#     # filter and prepare new configuration dictionary
#     new_config = {
#         key.replace('new_', ''): value
#         for key, value in new_config.items()
#         if key is not None and
#            value is not None and
#            key.startswith('new_')
#     }
#
#     old_config_json = old_config.model_dump()
#
#     # build configuration string
#     config = "\n\t".join([f'old: {key}:{value} -> {new_config[key] if key in new_config else "<unchanged>"}'
#                           for key, value
#                           in old_config_json.items()])
#
#     # project the new configuration ontop of the old config
#     old_config_json = {**old_config_json, **new_config}
#     logging.info(f'config: {config}')
#
#     new_config = old_config.model_validate(old_config_json)
#
#     # updated configuration
#     return new_config
#
#
# # @deprecated
# def show_state_column_info(state: State):
#     columns = state.columns
#     column_string = ", ".join([f'{column}' for column, value in columns.items()])
#     logging.info(f'columns = [{column_string}]')
#     return column_string
#
#
# # @deprecated
# def show_state_info(results: dict):
#     if not results:
#         raise FileExistsError(
#             f'results cannot be blank or null, please provide a list of paths and states in either as a list[str] paths or a dict[path] = state values')
#
#     if isinstance(results, List):
#         raise NotImplementedError(
#             "list not implemented yet, please use a dictionary of state paths and state key value pairs")
#
#     for state_file, state in results.items():
#         # fetch file stats
#         stat = os.stat(state_file)
#
#         # build configuration string
#         configuration_string = "\n\t".join([f'{key}:{value}'
#                                             for key, value
#                                             in state.config.model_dump().items()])
#
#         if state.columns:
#             columns_string = ", ".join([f'[{key}]' for key in state.columns.keys()])
#         else:
#             columns_string = ""
#
#         print(f'config: {configuration_string}')
#         print(f'columns: {columns_string}')
#         print(f'state row count: {implicit_count_with_force_count(state)}')
#         print(f'created on: {dt.fromtimestamp(stat.st_ctime)}, '
#               f'updated on: {dt.fromtimestamp(stat.st_mtime)}, '
#               f'last access on: {dt.fromtimestamp(stat.st_atime)}')
#
