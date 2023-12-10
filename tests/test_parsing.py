from processor import utils

# TODO use unit testing with pytest


a_a="""Here is a brief summary paragraph on places animals like to live in:

Animals like to live in habitats suited to their needs. Wild animals tend to prefer natural environments like forests, grasslands, wetlands, deserts etc. Domesticated animals are often kept as pets in homes or on farms. They like safe spaces with shelter, food, water and room to move around. The ideal home for an animal provides comfort, protects them from predators, and allows them to exhibit natural behaviors. With proper care, animals can thrive in a variety of environments.
""".strip()

b_a="""Here is a brief summary paragraph responding to why some animals only live in certain places:

Some animals are only found living in certain places due to adaptations that help them survive in those particular environments. For example, polar bears are found in icy, Arctic regions because they have thick fur and insulating fat to withstand the cold temperatures. Penguins are found in Antarctica because they are excellent swimmers adapted to the cold waters. Camels survive in hot, dry deserts due to their ability to go long periods without water. The adaptations and abilities of different animal species determine where they are able to successfully live.
""".strip()

c_a="""Here is a one paragraph summary response on farm animals:

Common farm animals include cows, pigs, chickens, sheep, goats, horses, and donkeys. These animals are raised on farms for meat, milk, eggs, wool, labor, transportation, recreation, and companionship. Cows provide beef and dairy products. Pigs provide pork. Chickens give eggs and meat. Sheep produce wool and meat. Goats offer milk and meat. Horses and donkeys are used for labor, transportation, and recreation on farms. A variety of farm animals are raised to provide food and other products for human use and enjoyment.
""".strip()


a_b, _ = utils.parse_response_strip_assistant_message(a_a)
b_b, _ = utils.parse_response_strip_assistant_message(b_a)
c_b, _ = utils.parse_response_strip_assistant_message(c_a)

a_c = a_a.replace('Here is a brief summary paragraph on places animals like to live in:\n\n', '')
b_c = b_a.replace('Here is a brief summary paragraph responding to why some animals only live in certain places:\n\n', '')
c_c = c_a.replace('Here is a one paragraph summary response on farm animals:\n\n', '')

a = a_b == a_c
b = b_b == b_c
c = c_b == c_c

assert(a)
assert(b)
assert(c)

print(f'{a}, {b}, {c}')