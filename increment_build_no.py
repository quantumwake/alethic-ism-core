import yaml

def increment_build_number(meta_yaml_path):
    with open(meta_yaml_path, 'r') as file:
        data = yaml.safe_load(file)

    build_number = data['build']['number']

    # Assuming the build number is an integer
    data['build']['number'] = build_number + 1

    with open(meta_yaml_path, 'w') as file:
        yaml.dump(data, file)

if __name__ == "__main__":
    increment_build_number('meta.yaml')
