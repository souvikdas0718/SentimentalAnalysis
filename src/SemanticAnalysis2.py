import itertools
import math
import re
import pymongo
from tabulate import tabulate

client = pymongo.MongoClient("mongodb+srv://root:root@datacluster.j83b5.mongodb.net/?retryWrites=true&w=majority")
articleTweetList = []
newsArticle_number_count = 0
canada_word_count = 0


def fetchReuterDataFromMongoDB(dbCollection):
    resultSet = list(dbCollection.find({}, {"_id": 0, "Date": 1, "News_Title": 1, "News": 1}))
    for entry in resultSet:
        articleTweetList.append(entry['News'])
    localFile = open("semanticAnalysis2.txt", "a+")
    localFile.write('\n'.join(map(str, articleTweetList)))


def frequency():
    global newsArticle_number_count, canada_word_count
    tabular_data = []
    filterWordList = ["Reuter"]
    filterWordList2 = ["CANADA", "RAIN", "COLD", "HOT"]

    file = open("semanticAnalysis2.txt", 'r')

    rain_word_count = 0
    cold_word_count = 0
    hot_word_count = 0
    for line in file.readlines():
        for word in line[:-1].split(" "):
            if word in filterWordList:
                newsArticle_number_count += 1
            if word.upper() in filterWordList2:
                if word.upper() == "CANADA":
                    canada_word_count += 1
                elif word.upper() == "RAIN":
                    rain_word_count += 1
                elif word.upper() == "COLD":
                    cold_word_count += 1
                elif word.upper() == "HOT":
                    hot_word_count += 1

    print("###########################################################################################################################")
    print("                    Total documents                                                        ", newsArticle_number_count, "                          ")
    print("###########################################################################################################################")
    tabular_data.append(["Canada", canada_word_count, newsArticle_number_count / canada_word_count,
                         math.log10(newsArticle_number_count / canada_word_count)])
    tabular_data.append(["Rain", rain_word_count, newsArticle_number_count / rain_word_count,
                         math.log10(newsArticle_number_count / rain_word_count)])
    tabular_data.append(["Cold", cold_word_count, newsArticle_number_count / cold_word_count,
                         math.log10(newsArticle_number_count / cold_word_count)])
    tabular_data.append(["Hot", hot_word_count, newsArticle_number_count / hot_word_count,
                         math.log10(newsArticle_number_count / hot_word_count)])
    print(tabulate(tabular_data, headers=["Search Query", "Document containing term(df)",
                                          "Total Documents(N)/ number of documents term appeared (df)", "Log10(N/df)"]))
    print("###########################################################################################################################")


def frequencyArticles():
    tabular_data = []
    file = open("semanticAnalysis2.txt", 'r')
    articleList = file.read().split("Reuter")
    articleSize = [len(x.split()) for x in articleList]
    canadaCount = [len(re.findall("canada ", x.lower())) for x in articleList]
    filteredArticleList = list(itertools.compress(zip(articleSize, canadaCount), canadaCount))
    print("######################################################################")
    print("Term                                           Canada                 ")
    print("######################################################################")
    for index, article in enumerate(filteredArticleList):
        tabular_data.append([f"Article #{index + 1}", article[0], article[1]])
    print(tabulate(tabular_data, headers=[f"Canada appeared in {len(filteredArticleList)} documents", "Total Words (m)",
                                          "Frequency (f )"]))

    article_with_highest_relative_frequency = max(tabular_data, key=lambda x: x[2] / x[1])
    print("######################################################################")
    print("  ", article_with_highest_relative_frequency[0], " has highest relative frequency: ",
          article_with_highest_relative_frequency[2] / article_with_highest_relative_frequency[1])
    print("######################################################################")


newsCollectionOne = client.ReuterDb.newsOne
fetchReuterDataFromMongoDB(newsCollectionOne)
newsCollectionTwo = client.ReuterDb.newsTwo
fetchReuterDataFromMongoDB(newsCollectionTwo)
frequency()
frequencyArticles()
