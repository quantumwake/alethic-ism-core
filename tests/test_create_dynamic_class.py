class BaseClass:
    def __init__(self, **kwargs):
        pass

    def process_input(self, input_dict: dict) -> dict:
        raise NotImplementedError()


def test_create_dynamic_class():
    # Define the content of the new class
    new_class_content = """class MyClass(BaseClass):
    def __init__(self, name, **kwargs):
        super().__init__(**kwargs)
        print(f"fuck yeah {name}")
        self.name = name

    def process_input(self, input_dict) -> dict:
        print("Processing input for", self.name)
        for key, value in input_dict.items():
            print(key, ":", value)
    """

    # Execute the combined class declaration and content in the global scope
    exec(new_class_content, globals())

    # Access the newly created class from the global scope
    MyClass = globals()['MyClass']

    # Instantiate the new class
    x = MyClass(name="bob is the name")

    # Use the instance method
    x.process_input({"hello": "world"})
