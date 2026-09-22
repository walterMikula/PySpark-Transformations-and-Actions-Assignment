from pyspark import SparkContext

## TASK 1
## CREATE RDD from [15, 22, 35, 42, 60, 18, 27, 19, 75, 29]
sc.SparkContext("local[*]", "RDD Practice")

ages = [15, 22, 35, 42, 60, 18, 27, 19, 75, 29]

ages_rdd = sc.parallelize(ages)

# assigning an age category
def categorize_age(ages):
    if age < 18:
        return "minor"
    elif age < 65: 
        return "adult"
    else:
        return "senior"

# applying categorize function to every age
age_categories = ages_rdd.map(categorize_age)

# count how many times each category appears
category_counts = age_categories.countByValue()

print("AGE CATEGORIES COUNTS: ")
print(category_counts)

adult_ages = ages_rdd.filter(lambda age: 18 <= age < 65)

print("\nADULT AGES:")
print(adult_ages.collect())

## TASK 2
## PROCESSING WORD DATA

sentences = [
    "love this product",
    "not worth the price",
    "highly recommend",
    "do not buy",
    "amazing quality",
    "very bad experience"
    ]

# creating rdd for sentences
sentences_rdd = sc.parallelize(sentences)

# using flatmap to split each sentence into words and creating an RDD with each individual word
words_rdd = sentences_rdd.flatMap(lambda sentence: sentence.split())


#count total number of words
total_words = words_rdd.count

print("\n TOTAL NUMBER OF WORDS")
print(total_words)


# convert each to a pair
word_pairs = words_rdd.map(lambda word: (word, 1))

#adds values that are the same word
word_frequencies = word_pairs.rteduceByKey(lambda x, y: x + y)

positive_words =[
    "love",
    "recommend",
    "amazing",
    "quality",
    "highly"
]

#filter the specific words
filtered_words = word_frequencies.filter(
    lambda pair: pair[0] in positive_words
)

#display the word frequency pairs
print("\nFILTERED WORD FREQUENCIES:")
print(filtered_words.collect())

## TASK 3

revenue_data = [
    ("hat", 25),
    ("shirt", 40),
    ("hat", 30),
    ("shoes", 80),
    ("shirt", 20)
]

#creating an RDD from revenue data
revenue_rdd = sc.parallelize(revenue_data)

# cp,bones revenue values for the same product
total_revenue = revenue_rdd.reduceByKey(lambda x, y: x + y)

# display revenue results
print("\n TOTAL REVENUE PER PRODCUT:")
print(total_revenue.collect())

# max will return the pair with highest revenue, and key compares the pairs
highest_grossing = revenue_rdd.max(key=lambda pair: pair[1])

print("\n HIGHEST-GROSSING PRODUCT")
print(highest_grossing)

sc.stop
