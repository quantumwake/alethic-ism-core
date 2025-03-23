import src.ismcore.utils.general_utils as utils


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