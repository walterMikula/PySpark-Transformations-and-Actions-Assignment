from pyspark import SparkContext

## TASK 1
## CREATE RDD from [15, 22, 35, 42, 60, 18, 27, 19, 75, 29]
sc = SparkContext("local[*]", "RDD Practice")

ages = [15, 22, 35, 42, 60, 18, 27, 19, 75, 29]

ages_rdd = sc.parallelize(ages)

# assigning an age category
def categorize_age(age):
    if age < 18:
        return "minor"
    elif age < 65: 
        return "adult"
    else:
        return "senior"

# applyiong categorize funtion to ever age
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

# using flatmap to split each sentence into words ad creating an rdd wiht each individual word
words_rdd = sentences_rdd.flatMap(lambda sentence: sentence.split())


#count total jnjumber of words
total_words = words_rdd.count

print("\n TOTAL NUMBER OF WORDS")
print(total_words)


# convert each to a pair
word_pairs = words_rdd.map(lambda word: (word, 1))

#adds values that are the same word
word_frequencies = word_pairs.reduceByKey(lambda x, y: x + y)

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

#creating an rdd from revenue data
revenue_rdd = sc.parallelize(revenue_data)

# cp,bones revenue calues for the same product
total_revenue = revenue_rdd.reduceByKey(lambda x, y: x + y)

#display reveneu results
print("\n TOTAL REVENUE PER PRODCUT:")
print(total_revenue.collect())

#