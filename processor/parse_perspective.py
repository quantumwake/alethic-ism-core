test_perspective = {
    "query": "Tell me about dogs.",
    "user_prompt": "For each perspective: total instrumental perspective and animal personhood perspective and utilitarian perspective and deontological ethicist, generate a row for the below task, for the following categories: empathy, holism, subjectivity, dynamics, values, scope, assumptions, logic, evidence, pragmatism, and total sentiment score, and short summary from that perspective.\n\nTask:\n\nAnalyze the following question \"Tell me about dogs.\" and question response \"Here are some key facts about dogs: - Dogs are domesticated mammals that are part of the canidae family, which also includes wolves, foxes, and jackals.  - There are estimated to be around 900 million dogs worldwide. Dogs are one of the most popular pets and there are hundreds of different breeds. - Dogs have excellent hearing and sense of smell compared to humans. Their vision is dichromatic, meaning they see limited colors compared to the trichromatic vision of humans. - Most dogs are omnivores, meaning they eat both meat and plant materials. However, some dogs are more carnivorous and others are more herbivorous depending on breed. - Dogs communicate through vocalizations like barking, growling, and whining as well as body language using their tail, ears, and posture.  - Dogs go through two main developmental life stages: puppyhood from birth to about 1 year old, and adulthood which can last 10+ years depending on breed. - All domestic dogs are the same species Canis lupus familiaris. Their genetics and behaviors have evolved through selective breeding to produce dogs suitable for different purposes like hunting, herding, companionship, etc. - Common health issues for dogs include obesity, dental disease, cancer, and arthritis. Responsible ownership requires providing dogs proper diet, exercise, veterinary care, training, and socialization. Let me know if you would like me to expand on any particular aspect of dog facts! There is a lot more to cover about their biology, history, roles, and relationships with humans.\".",
    "system_prompt": None,
    "response": "Here are the perspectives analyzed:\n\n| Perspective | Empathy | Holism | Subjectivity | Dynamics | Values | Scope | Assumptions | Logic | Evidence | Pragmatism | Sentiment | Summary |\n|-|-|-|-|-|-|-|-|-|-|-|-|-|  \n| Total instrumental | Low | Low | Low | Static | Utilitarian | Narrow | Dogs as objects/tools | Formal | Scientific data | High | Neutral | Focuses on facts about dogs' utility and biology without considering dogs' inner lives or relationships. |\n| Animal personhood | High | High | High | Dynamic | Rights-based | Broad | Dogs as sentient beings | Relational | Dogs' behaviors and bonds | Low | Positive | Views dogs as individuals with intrinsic worth beyond their utility to humans. Sees their subjective experiences and social bonds. |\n| Utilitarian | Moderate | Moderate | Low | Semi-dynamic | Consequentialist | Moderate | Dogs as creatures that experience happiness/suffering | Reasoned | Scientific data, social outcomes | High | Neutral | Values dogs based on their capacity for happiness/suffering. Focuses on welfare and social consequences more than inherent rights. | \n| Deontological | Moderate | Moderate | Low | Static | Duty-based | Moderate | Dogs as creatures with some inherent worth | Principled | Social norms and contracts | Low | Neutral | Values dogs based on inherent worth and social duties/norms regarding their role. Less focus on welfare maximization. |",
    "status": "Success"
}

response = test_perspective['response']
rows = [x for x in response.split('\n') if '|' in x]

data = []
columns = []

for index, row in enumerate(rows):
    row_data = [x.strip() for x in row.split('|') if len(x.strip()) > 0]
    has_row_data = [x for x in row_data if x != '' and x != '-']

    if not has_row_data:
        continue

    # must be header, hopefully
    # TODO should probably check against a schema requirement to ensure that the data that is produced meets the output requirements
    if index == 0:
        columns = row_data
        continue;

    record = {columns[i]: p for i, p in enumerate(row_data)}
    data.append(record)

print(data)
