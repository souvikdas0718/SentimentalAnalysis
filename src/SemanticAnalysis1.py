import pymongo
from tabulate import tabulate

mongoClient = pymongo.MongoClient("mongodb+srv://root:root@datacluster.j83b5.mongodb.net/?retryWrites=true&w=majority")
negativeWordList = []
positiveWordList = []
tweetList = []


def fetchProcessDataFromMongoDB(dbCollection):
    resultSet = list(
        dbCollection.find({}, {"_id": 0, "created_at": 1, "username": 1, "location": 1, "tweet": 1, "likes": 1,
                               "followers": 1, "following": 1}))
    for entry in resultSet:
        tweetList.append(entry['tweet'])

    localFile = open("semanticAnalysis1.txt", "a+")
    localFile.write('\n'.join(map(str, tweetList)))


def wordList():
    file = open("semanticAnalysis1.txt", 'r')
    generateNegativeAndPositiveWordList()
    tabular_data = []
    tweet_number = 0
    for tweet in file.readlines():
        tweet_number += 1
        wordBag = {}
        match = ""
        for word in tweet[:-1].split(" "):
            if wordBag.get(word) is None:
                wordBag[word] = 1
            else:
                wordBag[word] = wordBag[word] + 1

        negative_Count = 0
        positive_Count = 0
        for word in wordBag:
            if word in negativeWordList:
                match = match + ", " + word
                negative_Count += 1
            if word in positiveWordList:
                match = match + ", " + word
                positive_Count += 1

        match = match[1:]
        if negative_Count > positive_Count:
            polarity = "negative"
        elif negative_Count < positive_Count:
            polarity = "positive"
        else:
            polarity = "neutral"
            match = "NA"

        tabular_data.append([tweet_number, tweet, match, polarity])
    localFile = open("semanticAnalysis1_Output.txt", "a+")
    localFile.write((tabulate(tabular_data, headers=["Tweet", "Message/Tweets", "match", "popularity"])))


def generateNegativeAndPositiveWordList():
    file = open("negativeWords.txt", 'r')
    for negativeWord in file.readlines():
        negativeWordList.append(negativeWord[:-1])

    file = open("positiveWords.txt", 'r')
    for positiveWord in file.readlines():
        positiveWordList.append(positiveWord[:-1])


processDbCollection = mongoClient.ProcessDb.tweets
fetchProcessDataFromMongoDB(processDbCollection)
wordList()
