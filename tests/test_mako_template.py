import src.ismcore.utils.general_utils as utils

def test_make_template_with_set():
    # 1) define a super-simple template that loops over `items`
    tmpl = """
    Items:
    % for it in items:
      - Name: ${it['name']}, Age: ${it['age']}, Job: ${it['job']}
    % endfor
    """

    # 2) your data as a list of dicts
    data = [
        {'name': 'Alice', 'age': 30, 'job': 'Developer'},
        {'name': 'Bob',   'age': 25, 'job': 'Designer'},
        {'name': 'Carol', 'age': 28, 'job': 'Data Scientist'},
    ]

    # 3) render and print
    # output = render_list_template(tmpl, data)
    # Define a function to handle missing values
    def error_callback(context, key):
        if key.name == "items":
            return None

        return ""

    output = utils.build_template_text_mako(tmpl, data, error_callback=error_callback)
    print(output)

def test_mako_template_with_decimal():
    content = "hello world ${test_var} and ${context.get('3a')}"
    data = {
        "test_var": 123.5,
        "3a": "test",
    }

    # Define a function to handle missing values
    def error_callback(context, key):
        return ""

    rendered_content = utils.build_template_text_mako(
        template=content, data=data, error_callback=error_callback
    )

    print(rendered_content)

def test_mako_template():
    content = """
    Hello, ${user['name']}!

    Your Details:
    - Age: ${user['age']}
    - Occupation: ${user['job']}

    Your Hobbies:
    % for hobby in hobbies:
      - ${hobby}
    % endfor

    Your Address:
    ${address['street']}
    ${address['city']}, ${address['country']}

    Your Scores:
    % for subject, score in scores.items():
      - ${subject}: ${score}
    % endfor

    Family Members:
    % for member, details in family.items():
      - ${member.capitalize()}:
        Age: ${details['age']}
        Occupation: ${details['occupation']}
    % endfor
    """

    data = {
        'user': {
            'name': 'Alice',
            'age': 30,
            'job': 'Software Developer'
        },
        'hobbies': ['reading', 'hiking', 'photography'],
        'address': {
            'street': '123 Python Street',
            'city': 'Codevillle',
            'country': 'Pythonia'
        },
        'scores': {
            'Math': 95,
            'Science': 88,
            'Literature': 92
        },
        'family': {
            'mother': {'age': 55, 'occupation': 'Teacher'},
            'father': {'age': 58, 'occupation': 'Engineer'},
            'sister2': {'age': 25, 'occupation': 'Artist'}
        }
    }

    # Define a function to handle missing values
    def error_callback(context, key):
        if key.name == "items":
            return None

        return ""

    rendered_content = utils.build_template_text_mako(
        template=content, data=data, error_callback=error_callback
    )

    print (rendered_content)