from fastapi import FastAPI
from pydantic import BaseModel
import random
app = FastAPI()


class facts(BaseModel):
    id: int
    fact : str


fact_list = [facts(id= 1, fact = "'Strengths' is the longest word with only one vowel. "),
facts(id = 2, fact = "Redheads typically need about 20% more anesthesia than people with different hair color."),
facts(id = 3,
fact = "As of 2024, Madagascar has a larger population than Australia."),
facts(id = 4,
fact = "The most frequently purchased items on Amazon (2024) are the Apple Air Tags and the Stanley tumbler."),
facts(id = 5,
fact = "Google Images was inspired after Jennifer Lopez wore a then-scandalous green dress to the 2000 Grammy Awards. Google searches for that image became the most popular search query Google had ever seen, and they realized they needed a way to deliver what people wanted."),
facts(id = 6,
fact =  "The first free public library was founded in Peterborough, New Hampshire in 1833."),
facts(id = 7,
fact = "Dr. Seuss's book Green Eggs and Ham was banned in China because it was thought to portray Marxism in a negative light with the way the eggs and ham are forced on others."),
facts(id = 8,
fact =  "A single litter of kittens can have more than one father."),
facts(id =9,
fact =  "After the U.S. president and vice president, the postmaster general is the next highest-paid federal government employee (about $275k starting salary)."),
facts(id = 10,
fact = "Lake Chargo-ggagoggm-anchaug-gagoggcha-ubunagun gamaugg, in the town of Webster, Massachusetts, is the longest place-name in the United State. It is also known as Webster Lake."),
facts(id = 11,
fact = "Even though the Statue of Liberty is on Liberty Island, which belongs to New York, it is actually located closer to the land mass of New Jersey."),
facts(id =12,
fact = "Every time you shuffle a randomized deck of cards, it is very likely that you have created the first ever arrangement of cards in that order in history. Based on 52 cards, there are 8.07x10^67 possible arrangements of cards."),
facts(id = 13,
fact = "Toilet paper is thought to have first been produced in the late 800's AD in China."),
facts(id = 14,
fact =  "The squid's brain is torus-shaped (ring-shaped), with its esophagus running through it. If it eats anything too large, it can suffer brain damage."),
facts( id = 15,
fact = "The French surgeon Serge Voronoff is famous for his work in the 1920's of grafting monkey testicle tissue onto the testicles of men. This was thought to have a rejuvenating effect, and around 2,000 men had the procedure done."),
facts(id = 16,
fact = "The deepest hole ever drilled by man is the Kola Superdeep Borehole (40,226 feet)."),
facts(id = 17,
fact = "Bananas contain potassium, which is an element that decays, making bananas the slightest bit radioactive. But you would probably have to consume ten million bananas worth of potassium in order to die of banana-induced radiation poisoning."),
facts(id =  18,
fact = "A bear enlisted in the Polish army made it to the rank of Corporal. He also smoked, drank and carried weapons to the front during battles. His name was Wojtek."),
facts(id = 19,
fact = "The comparative size between an atom and a grain of sand is about the same ratio as that between a grain of sand and planet Earth."),
facts(id = 20,
fact =  "There are only two doubly-landlocked countries in the world, meaning they are a landlocked country surrounded on all sides by landlocked countries. They are Liechtenstein and Uzbekistan."),
facts(id = 21,
fact = "In 1987, American Airlines saved $40,000 annually by removing one olive from their first class salads."),
facts(id = 22,
fact =  "Before becoming a famous painter, Bob Ross was a military drill sergeant."),
facts(id = 23,
fact = "There are around 200 corpses on Mount Everest, many of which are used as markers for climbers."),
facts( id = 24,
fact =  "A restaurant in Pensacola, Florida called McGuire Irish Pub has more than $500,000 worth of dollar bills hanging from the walls and ceiling."),
facts(id = 25,
fact = "The old distress signal \"SOS\" does not stand for \"Save Our Souls\" or \"Save Our Ships.\" The letters SOS were chosen because they're the easiest Morse Code letters to remember and use: three dots, three dashes, three dots.")
]


# GET method for all
@app.get("/")
async def get_all():
    return fact_list

# GET method by random
@app.get("/fact")
async def get():
    rand_num = random.randrange(1, len(fact_list))
    return fact_list[rand_num]


# GET method by fact id
@app.get("/fact?id={id}/")
async def get_by_id(id: int):
    return next((i for i in fact_list if i.id == int(id)), None)

# GET method by fact id
@app.get("/fact/{id}/")
async def get_by_fact_id(id : int):
    return next((i for i in fact_list if i.id == int(id)), None)

# POST method for adding todo list
@app.post("/add_fact/")
async def add(newfact: facts):
    fact_list.append(newfact)
    return fact_list


# DELETE method to delete by id
# @app.delete("/{id}/")
# async def delete(id):
#     for a in todos:
#         if a.todo_id == int(id):
#             todos.remove(a)
#     return None