from textblob import TextBlob

def get_sentiment(text):
    # Create a TextBlob object
    blob = TextBlob(text)

    # Get sentiment polarity (-1 to 1, where < 0 is negative, > 0 is positive)
    sentiment = blob.sentiment.polarity
    print(sentiment)
    if sentiment>0:
        return "positive"
    elif sentiment==0:
        return "neutral"
    else:
        return "negative"
