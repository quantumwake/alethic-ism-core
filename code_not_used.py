
def merge_content_by_alternating_role(self, message_list: list[dict]):
    # reorganize the message_list[dict] such that if there are redundant entries, it merges them into a single
    # entry, it has to be alternating between role of user and assistant, as such, we batch the messages into a
    # single message if prior entry was of the same role as the current entry being evaluated.
    new_message_list = []
    for ce in message_list:
        if not new_message_list:
            new_message_list.append(ce)
            continue

        role = ce['role']
        user = ce['user']
        content = ce['content']

        # if the previous entry's role is the same as the current message's role, then merge the content
        previous_entry = new_message_list[-1]
        if previous_entry and role == previous_entry['role']:
            # update the last entry by appending the contents of this entry to it
            last_content = previous_entry['content']
            last_content = f"{user}: {last_content}\n\n{content}"
            new_message_list[-1]['content'] = last_content

    return new_message_list

def derive(self, template: str, input_data: [str, dict, list]):
    if not isinstance(input_data, dict):
        raise NotImplementedError('sorry this is not implemented on this controller yet, '
                                  'you can only use input data of query_state type and structure')

    # Prepare the message dictionary with the query_string
    if 'session_id' not in input_data:
        message = {"role": "user", "content": template.strip()}
        return message

    # if there are no messages in the session then add rendered template as the first message
    merged_message_list = []
    message_list = self.fetch_session_data(input_data)
    if message_list:
        merged_message_list = self.merge_content_by_alternating_role(message_list=message_list)

    # append the new input message to the history and return
    user = input_data['user']
    role = input_data['role']
    content = f"{user}: {template.strip()}"
    new_message = {"role": role, "content": f"{user}: {content}"}
    merged_message_list.append(new_message)
    return merged_message_list
